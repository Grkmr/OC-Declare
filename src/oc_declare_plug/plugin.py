import shutil
import tempfile
from pathlib import Path
from typing import Annotated, Literal

import oc_declare
from oc_declare import OCDeclareArc
from ocelescope import (
    OCEL,
    OCEL_FIELD,
    OCELAnnotation,
    Plugin,
    PluginInput,
    Resource,
    Table,
    TableColumn,
    plugin_method,
)
from pydantic import BaseModel, Field


class Constraint(BaseModel):
    type: Literal["AS", "EF", "EP", "DF", "DP"]
    source: str
    target: str
    any_objects: list[str] = []
    all_objects: list[str] = []
    each_objects: list[str] = []
    min: int | None
    max: int | None
    conformance: float | None = None


def map_ocdeclarearc_to_constraint(arc: OCDeclareArc) -> Constraint:
    return Constraint(
        type=arc.arc_type_name,
        source=arc.from_activity,
        target=arc.to_activity,
        any_objects=arc.any_ots,
        all_objects=arc.all_ots,
        each_objects=arc.each_ots,
        min=arc.min_count,
        max=arc.max_count,
    )


class Constraints(Resource):
    label = "OC-DECLARE Constraints"
    description = "A list of discovered OC-DECLARE constraints"

    constraints: list[Constraint]

    def visualize(self) -> Table:
        columns = [
            TableColumn(id="type", label="Type", sortable=True),
            TableColumn(id="source", label="Source Activity", sortable=True),
            TableColumn(id="target", label="Target Activity", sortable=True),
            TableColumn(id="all", label="ALL"),
            TableColumn(id="each", label="EACH"),
            TableColumn(id="any", label="ANY"),
            TableColumn(id="min", label="Min Count", data_type="number"),
            TableColumn(id="max", label="Max Count", data_type="number"),
        ]

        # Add optional conformance column
        if any(c.conformance is not None for c in self.constraints):
            columns.append(TableColumn(id="conformance", label="Conformance", data_type="number"))

        rows = []
        for c in self.constraints:
            row = {
                "type": c.type,
                "source": c.source,
                "target": c.target,
                "all": ", ".join(c.all_objects),
                "any": ", ".join(c.any_objects),
                "each": ", ".join(c.each_objects),
                "min": c.min,
                "max": c.max,
            }
            if c.conformance is not None:
                row["conformance"] = c.conformance
            rows.append(row)

        return Table(columns=columns, rows=rows)


class DiscoverInput(PluginInput):
    threshold: float = Field(default=0.2, gt=0, le=1)
    acts_to_use: list[str] = OCEL_FIELD(
        field_type="event_type",
        title="Acitvities to use",
        ocel_id="ocel",
    )
    o2o_mode: Literal[
        "None",
        "Direct",
        "Reversed",
        "Bidirectional",
    ] = "None"

    check_conformance: bool = False


class ConstraintInput(BaseModel):
    type: Literal["AS", "EF", "EP", "DF", "DP"] = Field(title="Constraint Type")
    source: str = OCEL_FIELD(field_type="event_type", ocel_id="ocel", title="Source Activity")
    target: str = OCEL_FIELD(field_type="event_type", ocel_id="ocel", title="Target Activity")
    any_objects: list[str] = OCEL_FIELD(field_type="object_type", ocel_id="ocel", title="ALL")
    all_objects: list[str] = OCEL_FIELD(field_type="object_type", ocel_id="ocel", title="ANY")
    each_objects: list[str] = OCEL_FIELD(field_type="object_type", ocel_id="ocel", title="EACH")
    min: list[int] = Field(max_length=1, title="Minimum Count")
    max: list[int] = Field(max_length=1, title="Maximum Count")


class CreateConstraintsInput(PluginInput):
    constraints: list[ConstraintInput] = Field(
        title="Constraints to Create",
        description="List of manually defined OC-DECLARE constraints",
        default=[],
    )
    check_conformance: bool = Field(
        default=False,
        title="Check Conformance Automatically",
        description="If enabled, each manually created constraint will be evaluated for conformance against the event log.",
    )


def check_conformance_for_constraints(processed, constraints_resource: Constraints) -> Constraints:
    """
    Updates a Constraints resource in-place with conformance scores for each constraint.

    Parameters
    ----------
    ocel : OCEL
        The event log to check against.
    constraints_resource : Constraints
        A Constraints resource whose Constraint objects will be updated with conformance values.
    """

    # 3️⃣ Iterate through all constraints and compute conformance
    for c in constraints_resource.constraints:
        try:
            arc = oc_declare.OCDeclareArc(
                c.source,
                c.target,
                c.type,
                c.min,
                c.max,
                all_ots=c.all_objects,
                each_ots=c.each_objects,
                any_ots=c.any_objects,
            )

            score = oc_declare.check_conformance(processed, arc)
            c.conformance = round(score, 3)  # ✅ write result into constraint

        except Exception as e:
            print(f"⚠️ Failed to check conformance for {c.source} → {c.target}: {e}")
            c.conformance = None

    return constraints_resource


class OcDeclare(Plugin):
    label = "OC Declare"
    description = "Object-Centric Declare"
    version = "0.1.1"

    @plugin_method(label="Discover Constraints", description="Discover Constraints")
    def discover_constraints(
        self,
        ocel: Annotated[OCEL, OCELAnnotation(label="Event Log")],
        input: DiscoverInput,
    ) -> Constraints:
        with tempfile.NamedTemporaryFile(suffix=".jsonocel", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        ocel.write_ocel(tmp_path, ".jsonocel")
        json_path = tmp_path.with_suffix(".json")

        shutil.move(tmp_path, json_path)

        processed = oc_declare.import_ocel2(str(json_path))

        arcs = oc_declare.discover(processed, input.threshold, acts_to_use=input.acts_to_use, o2o_mode=input.o2o_mode)

        constraints = []

        for arc in arcs:
            c = map_ocdeclarearc_to_constraint(arc)
            if input.check_conformance:
                score = oc_declare.check_conformance(processed, arc)
                c.conformance = round(score, 3)
            constraints.append(c)

        return Constraints(constraints=constraints)

    @plugin_method(label="Create Constraints", description="Manually define OC-DECLARE constraints")
    def create_constraints(
        self,
        ocel: Annotated[OCEL, OCELAnnotation(label="Event Log")],
        input: CreateConstraintsInput,
    ) -> Constraints:
        """
        Create OC-DECLARE constraints manually from user input.
        """
        with tempfile.NamedTemporaryFile(suffix=".jsonocel", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        ocel.write_ocel(tmp_path, ".jsonocel")
        json_path = tmp_path.with_suffix(".json")

        shutil.move(tmp_path, json_path)

        processed = oc_declare.import_ocel2(str(json_path))

        constraints = [
            Constraint(
                type=c.type,
                source=c.source,
                target=c.target,
                all_objects=c.all_objects,
                each_objects=c.each_objects,
                any_objects=c.any_objects,
                min=c.min[0] if len(c.min) == 1 else None,
                max=c.max[0] if len(c.max) == 1 else None,
            )
            for c in input.constraints
        ]
        if input.check_conformance:
            return check_conformance_for_constraints(processed, Constraints(constraints=constraints))

        return Constraints(constraints=constraints)

    @plugin_method(label="Check Constraints", description="Check conformance on constraints")
    def check_constraints(
        self,
        ocel: Annotated[OCEL, OCELAnnotation(label="Event Log")],
        constraints: Constraints,
    ) -> Constraints:
        """
        Create OC-DECLARE constraints manually from user input.
        """
        with tempfile.NamedTemporaryFile(suffix=".jsonocel", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        ocel.write_ocel(tmp_path, ".jsonocel")
        json_path = tmp_path.with_suffix(".json")

        shutil.move(tmp_path, json_path)

        processed = oc_declare.import_ocel2(str(json_path))

        return check_conformance_for_constraints(processed, constraints)
