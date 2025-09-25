import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import LineString


class BuildScenarioLinks(object):
    def __init__(
        self,
        scenario_edges,
        scenario_junctions,
        time_period,
        config,
        logger,
        reversible_both_dir=False,
        reversible_switch_dir=False,
    ):
        self.network_gdf = scenario_edges
        self.junctions_gdf = scenario_junctions
        self.time_period = time_period
        self.config = config
        self.reversible_both_dir = reversible_both_dir
        self.reversible_switch_dir = reversible_switch_dir
        self._logger = logger
        self.full_network = self.create_full_model_network()
        self.junctions = self._create_junctions()

    def _create_junctions(self):
        junctions = pd.concat(
            [self.junctions_gdf, pd.DataFrame(columns=self.config.emme_node_columns)]
        )
        junctions["i"] = junctions["ScenarioNodeID"]
        junctions["is_zone"] = np.where(
            junctions["ScenarioNodeID"] <= int(self.config.max_zone_number), 1, 0
        ).astype(int)
        junctions = junctions[self.config.emme_node_columns + ["geometry"]]
        return junctions

    def create_full_model_network(self):
        # add columns for emme
        network = pd.concat(
            [self.network_gdf, pd.DataFrame(columns=self.config.emme_link_columns)]
        )

        # because a two way GP lane does not always have two way GP
        # JI hov oneway attributes will be configured below
        hov_edges = self._update_hov_oneway(network)
        network.update(hov_edges)

        # upadte all attributes, then do special ones later
        network = self._configure_standard_attributes(
            network, self.config.standard_links
        )

        # switch one way JI to IJ
        ji_edges = self._switch_oneway_ji(network)
        # configure oneway JI attributes
        ji_edges = self._configure_standard_attributes(
            ji_edges, self.config.standard_links
        )
        network.update(ji_edges)

        # create reverse links for two way streets
        reverse_links = self._create_reverse_links(network)
        reverse_links = self._configure_standard_attributes(
            reverse_links, self.config.standard_links
        )
        network = pd.concat([network, reverse_links])
        network.reset_index(inplace=True)

        # configure HOV attributes
        # hov_edges = network[network['FacilityType'] == 999]
        hov_edges = network[network["is_managed"] == 1]
        hov_edges = self._configure_hov_attributes(hov_edges)
        network.update(hov_edges)

        # configure ferry, rail
        network = self._configure_transit_links(network)

        # configure BAT links
        network = self._configure_BAT_links(network, self.config.bat_links)

        # reverse reversibles?
        if self.reversible_switch_dir:
            reversibles = self._reverse_reversibles(network)
            reversibles = self._configure_standard_attributes(
                reversibles, self.config.standard_links
            )
            network.update(reversibles)

        # create reverse walk links on one_way arterials/collectors
        reverse_walk_links = self._create_reverse_walk_links(network)
        reverse_walk_links = self._configure_emme_walk_attributes(
            reverse_walk_links, self.config.walk_links
        )

        # find duplicate links due to OSM network having one-way streets sharing samee IJ JI
        reverse_walk_links["id"] = (
            reverse_walk_links.i.astype(str) + "-" + reverse_walk_links.j.astype(str)
        )
        network["id"] = network.i.astype(str) + "-" + network.j.astype(str)
        reverse_walk_links = reverse_walk_links[
            ~reverse_walk_links["id"].isin(network["id"])
        ]

        network = pd.concat([network, reverse_walk_links])
        network.reset_index(drop=True, inplace=True)

        # configure weave link attributes
        weave_links = network[network["FacilityType"] == 98]
        weave_links = self._configure_weave_link_attributes(
            weave_links, self.config.weave_links
        )
        network.update(weave_links)

        # cpnfigure HOT lane tolls
        for k, v in self.config.hot_tolls.items():
            hot_links = network[
                (network["IJLanesHOV" + self.time_period] == k)
                & (network["is_managed"] == 1)
            ]
            hot_links = self._configure_hot_lane_tolls(hot_links, v)
            network.update(hot_links)

        network["bkfac"] = network["IJBikeType"]

        network = network[
            self.config.emme_link_columns + self.config.additional_keep_columns
        ]
        network.i = network.i.astype(int)
        network.j = network.j.astype(int)
        network["id"] = network.i.astype(str) + "-" + network.j.astype(str)
        network.set_index(network.id, inplace=True)

        network = self._validate_network(network)

        return network

    def _update_hov_oneway(self, network):
        # A two way GP lane does not always have two way GP
        hov_edges = network[network["FacilityType"] == 999]
        # One way IJ HOV:
        hov_edges["Oneway"] = np.where(
            (hov_edges["Oneway"] == 2)
            & (hov_edges["IJLanesHOV" + self.time_period] > 0)
            & (hov_edges["JILanesHOV" + self.time_period] == 0),
            0,
            hov_edges["Oneway"],
        )
        # One way JI HOV
        hov_edges["Oneway"] = np.where(
            (hov_edges["Oneway"] == 2)
            & (hov_edges["IJLanesHOV" + self.time_period] == 0)
            & (hov_edges["JILanesHOV" + self.time_period] > 0),
            1,
            hov_edges["Oneway"],
        )
        # Can have IJ GP and JI hov or vice versa
        hov_edges["Oneway"] = np.where(
            (hov_edges["Oneway"] == 0)
            & (hov_edges["IJLanesHOV" + self.time_period] == 0)
            & (hov_edges["JILanesHOV" + self.time_period] > 0),
            1,
            hov_edges["Oneway"],
        )
        hov_edges["Oneway"] = np.where(
            (hov_edges["Oneway"] == 1)
            & (hov_edges["IJLanesHOV" + self.time_period] > 0)
            & (hov_edges["JILanesHOV" + self.time_period] == 0),
            0,
            hov_edges["Oneway"],
        )
        return hov_edges

    def _switch_oneway_ji(self, network):
        ji_edges = network[network["Oneway"] == 1]
        flipped_geom = ji_edges.geometry.apply(self._flip_edges)
        # update with flipped geometry
        ji_edges.geometry.update(flipped_geom)
        cols = self._switch_attributes_dict()

        ji_edges = ji_edges.rename(columns=cols)
        ji_edges["Oneway"] = 0
        return ji_edges

    def _reverse_reversibles(self, network):
        reversibles = network[network["Oneway"] == 3]
        flipped_geom = reversibles.geometry.apply(self._flip_edges)
        # update reversibles with flipped geometry
        reversibles.geometry.update(flipped_geom)
        # now switch attributes
        # switch_columns = [x[1] + x[0] + x[2:] for x in self.config['dir_columns']]
        # rename_dict = dict(zip(self.config['dir_columns'], switch_columns))
        ## also INode and Jnode
        # rename_dict['NewINode'] = 'NewJNode'
        # rename_dict['NewJNode'] = 'NewINode'
        reversibles = reversibles.rename(columns=self._switch_attributes_dict())
        return reversibles

    def _create_reverse_links(self, network):
        if self.reversible_both_dir:
            two_way_edges = network[(network["Oneway"] == 2) | (network["Oneway"] == 3)]
        else:
            two_way_edges = network[network["Oneway"] == 2]
        # flip geometry
        flipped_geom = two_way_edges.geometry.apply(self._flip_edges)
        two_way_edges.geometry.update(flipped_geom)
        # switch IJ & JI attributes for edges that are digitized in the opposite direction of the project
        cols = self._switch_attributes_dict()
        two_way_edges = two_way_edges.rename(columns=cols)
        return two_way_edges

    def _create_reverse_walk_links(self, network):
        reverse_walk_links = network[
            network.FacilityType.isin(self.config.reverse_walk_link_facility_types)
        ]
        reverse_walk_links = reverse_walk_links[
            (reverse_walk_links["Oneway"] == 0) | (reverse_walk_links["Oneway"] == 1)
        ]
        reverse_walk_links = reverse_walk_links[reverse_walk_links["is_managed"] == 0]
        flipped_geom = reverse_walk_links.geometry.apply(self._flip_edges)
        reverse_walk_links.geometry.update(flipped_geom)
        cols = self._switch_attributes_dict()
        reverse_walk_links = reverse_walk_links.rename(columns=cols)
        # set these these all to oneway IJ-dont want them to get picked up by a JI query later on
        reverse_walk_links["Oneway"] = 0
        return reverse_walk_links

    def _flip_edges(self, geometry):
        line = list(geometry.coords)
        line.reverse()
        return LineString(line)

    def _switch_attributes_dict(self):
        cols = self.config.dir_columns + self.config.dir_toll_columns
        switch_columns = [x[1] + x[0] + x[2:] for x in cols]
        rename_dict = dict(zip(cols, switch_columns))
        # also INode and Jnode
        rename_dict["NewINode"] = "NewJNode"
        rename_dict["NewJNode"] = "NewINode"
        return rename_dict

    def _configure_emme_walk_attributes(self, edges, look_up_dict):
        edges.direction = int(look_up_dict["direction"])
        edges.i = edges[look_up_dict["i"]]
        edges.j = edges[look_up_dict["j"]]
        edges.length = edges.geometry.length / 5280.0
        edges.modes = look_up_dict["modes"]
        edges.type = look_up_dict["type"]
        edges.lanes = look_up_dict["lanes"]
        edges.vdf = look_up_dict["vdf"]
        edges.ul1 = look_up_dict["ul1"]
        edges.ul2 = look_up_dict["ul2"]
        edges.ul3 = look_up_dict["ul3"]
        edges.toll1 = look_up_dict["toll1"]
        edges.toll2 = look_up_dict["toll2"]
        edges.toll3 = look_up_dict["toll3"]
        edges.trkc1 = look_up_dict["trkc1"]
        edges.trkc2 = look_up_dict["trkc2"]
        edges.trkc3 = look_up_dict["trkc3"]

        return edges

    def _configure_weave_link_attributes(self, edges, look_up_dict):
        edges.direction = int(look_up_dict["direction"])
        edges.i = edges[look_up_dict["i"]]
        edges.j = edges[look_up_dict["j"]]
        edges.length = look_up_dict["length"]
        edges.modes = look_up_dict["modes"]
        edges.type = look_up_dict["type"]
        edges.lanes = look_up_dict["lanes"]
        edges.vdf = look_up_dict["vdf"]
        edges.ul1 = look_up_dict["ul1"]
        edges.ul2 = look_up_dict["ul2"]
        edges.ul3 = look_up_dict["ul3"]
        edges.toll1 = look_up_dict["toll1"]
        edges.toll2 = look_up_dict["toll2"]
        edges.toll3 = look_up_dict["toll3"]
        edges.trkc1 = look_up_dict["trkc1"]
        edges.trkc2 = look_up_dict["trkc2"]
        edges.trkc3 = look_up_dict["trkc3"]

        return edges

    def _configure_standard_attributes(self, edges, look_up_dict):
        edges.direction = int(look_up_dict["direction"])
        edges.i = edges[look_up_dict["i"]]
        edges.j = edges[look_up_dict["j"]]
        edges.length = edges.geometry.length / 5280.0
        edges.modes = edges[look_up_dict["modes"]]
        edges.type = edges[look_up_dict["type"]]
        edges.lanes = edges[look_up_dict["lanes"] + self.time_period]
        edges.vdf = edges[look_up_dict["vdf"]]
        edges.ul1 = edges[look_up_dict["ul1"]]
        edges.ul2 = edges[look_up_dict["ul2"]]
        edges.ul3 = edges[look_up_dict["ul3"]]
        edges.toll1 = edges[look_up_dict["toll1"] + self.time_period]
        edges.toll2 = edges[look_up_dict["toll2"] + self.time_period]
        edges.toll3 = edges[look_up_dict["toll3"] + self.time_period]
        edges.trkc1 = edges[look_up_dict["trkc1"] + self.time_period]
        edges.trkc2 = edges[look_up_dict["trkc2"] + self.time_period]
        edges.trkc3 = edges[look_up_dict["trkc3"] + self.time_period]
        edges.ttf = edges[look_up_dict["ttf"]]

        return edges

    def _configure_hov_attributes(self, edges):
        # first recode modes to the HOV lanes field, which is a lookup for type of HOV and number of lanes
        edges.modes = edges["IJLanesHOV" + self.time_period]
        # Now use dict to recode
        edges.modes.replace(self.config.hov_modes, inplace=True)
        edges.lanes = edges["IJLanesHOV" + self.time_period]
        edges.lanes.replace(self.config.hov_lanes, inplace=True)
        edges.ul1 = edges[self.config.hov_capacity]
        edges.i = edges.NewINode
        edges.j = edges.NewJNode
        return edges

    def _configure_hot_lane_tolls(self, edges, col_list):
        rate = self.config.hot_rate_dict[self.time_period]
        for col in col_list:
            edges[col] = edges[col] + (rate * edges.length) / 5280
        return edges

    def _configure_transit_links(self, edges):
        edges.ul2 = edges.ul2.astype(float)
        edges.ul2 = np.where(
            edges["FacilityType"].isin(self.config.link_time_facility_types),
            edges.Processing_x / 1000.0,
            edges.ul2,
        )
        return edges

    def _configure_BAT_links(self, edges, look_up_dict):
        # ID BAT lanes by there mode strng
        modes = self.config.hov_modes[3]
        edges.vdf = np.where(edges["modes"] == modes, look_up_dict["vdf"], edges.vdf)
        # edges.FacilityType = np.where(edges['modes']==modes, 0, edges.FacilityType)
        edges.ul1 = np.where(edges["modes"] == modes, look_up_dict["ul1"], edges.ul1)
        edges.ul3 = np.where(edges["modes"] == modes, look_up_dict["ul3"], edges.ul3)
        return edges

    def _validate_network(self, edges):
        if len(edges[edges["type"] == 0]) > 0:
            self._logger.warning(
                "Warning: Field LinkType in TransRefEdges containes 0s. Recoding to 90."
            )
            edges["type"] = np.where(edges["type"] == 0, 90, edges["type"])
        # fix this
        edges["type"] = np.where(edges["type"] > 90, 90, edges["type"])

        loops = edges[edges["i"] == edges["j"]].PSRCEdgeID.tolist()
        if loops:
            for edge_id in loops:
                self._logger.warning(
                    "Warning: Edge %s has the same i and j node." % (edge_id)
                )

        empty_mode = edges[edges["modes"] == " "].PSRCEdgeID.tolist()
        if empty_mode:
            for edge_id in empty_mode:
                self._logger.warning(
                    "Warning: Edge %s mode field is blank. Please fix!" % (edge_id)
                )

        duplicates = edges[edges["id"].value_counts() > 1]["id"].tolist()
        if duplicates:
            for duplicate in duplicates:
                self._logger.warning(
                    "Warning: Edge %s has duplicates. Please fix!" % (duplicates)
                )

        return edges
