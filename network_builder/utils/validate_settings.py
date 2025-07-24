from pydantic import BaseModel, validator
from typing import List, Optional
import typing
from pathlib import Path


class ValidateSettings(BaseModel):
    # Data sources
    data_source_type: str
    export_to_file_gdb: bool
    sde_schema: str
    use_sqlalchemy: typing.Any = None
    server: typing.Any = None
    database: typing.Any = None
    version: typing.Any = None
    file_gdb_path: typing.Any = None

    # Standard user params:
    output_dir: str
    number_of_pools: int
    model_year: int
    input_crs: int
    output_crs: typing.Any = None
    main_log_file: str
    max_zone_number: int
    node_offset: int
    max_regular_zone: int
    time_periods: list
    add_channelization: bool
    transit_version: int

    # Projects:
    update_network_from_projects: bool
    scenario_name: str
    projects_version_year: int
    project_buffer_dist: int

    # Others:
    # Facilities where slope should be calculated; slope of 0 is assumed otherwise
    bike_facility_types: list
    hov_shift_dist: float
    build_transit_headways: bool
    build_bike_network: bool
    save_network_files: bool
    create_emme_network: bool
    export_build_files: bool
    emme_folder_name: str
    emmebank_title: str
    submode_dict: dict

    # Bike Network
    elev_conversion: float  # Convert raster elevation from meters to feet
    raster_file_path: str
    ferry_link_factor: int

    transit_headway_mapper: dict
    reversibles: dict
    extra_attributes: dict
    dir_columns: list
    toll_columns: list
    dir_toll_columns: list
    non_dir_columns: list
    project_columns: list
    project_update_columns: list
    intermediate_keep_columns: list
    emme_link_columns: list
    additional_keep_columns: list
    emme_node_columns: list
    link_time_facility_types: list
    hot_rate_dict: dict
    hot_tolls: dict
    walk_links: dict
    weave_links: dict
    standard_links: dict
    bat_links: dict
    hov_capacity: str
    hov_lanes: dict
    hov_modes: dict
    reverse_walk_link_facility_types: list
    standard_facility_types: list

    @validator("data_source_type")
    def method_is_valid(cls, method: str) -> str:
        allowed_set = {"enterprise_gdb", "file_gdb"}
        if method not in allowed_set:
            raise ValueError(f"must be in {allowed_set}, got '{method}'")
        return method

    @validator("use_sqlalchemy", always=True)
    def enterprise_gdb_required_fields_boolean(cls, v, values, **kwargs):
        if values["data_source_type"] == "enterprise_gdb":
            if type(v) != bool:
                raise ValueError(
                    f"Field must be boolean when using enterprise_gdb as data source!"
                )
            else:
                return v

    @validator("server", "database", "version", always=True)
    def enterprise_gdb_required_fields_string(cls, v, values, **kwargs):
        if values["data_source_type"] == "enterprise_gdb":
            if type(v) != str:
                raise ValueError(
                    f"Field must be boolean when using enterprise_gdb as data source!"
                )
            else:
                return v

    @validator("file_gdb_path", always=True)
    def file_gdb_required_fields_string(cls, v, values, **kwargs):
        if values["data_source_type"] == "file_gdb":
            if type(v) != str:
                raise ValueError(
                    f"Field must be boolean when using enterprise_gdb as data source!"
                )
            else:
                return v


class ValidateTableSettings(BaseModel):
    mode_attributes: Optional[str] = None

    mode_tolls: str

    edges: str

    transit_lines: str

    transit_points: str

    turn_movements: str

    junctions: str

    transit_frequencies: str

    project_routes: str

    projects_in_scenarios: str

    project_attributes: str

    point_events: str

    zones: str

    @validator("mode_attributes")
    def prevent_none(cls, v):
        assert v is not None, "size may not be None"
        return v
