import networkx as nx
import matplotlib.pyplot as plt
from arcticdb.toolbox.library_tool import LibraryTool, KeyType
import numpy as np
import pandas as pd
from IPython.display import display, Markdown
from .library_utils import check_and_adapt_library, check_symbol_exists
from enum import Enum
from arcticdb.supported_types import time_types as supported_time_types


class VersionExistence(Enum):
    EXISTS_IN_VERSION_LIST = "exists_in_version_list"
    EXISTS_AS_COMPACTED_VERSION = "exists_as_compacted_version"
    EXISTS_ONLY_IN_SNAPSHOTS = "exists_in_snapshots"
    NOT_EXISTS = "not_exists"


def get_diagnosis_description(
    version_exists,
    is_tombstoned,
    target,
    steps,
    snapshots_count,
    snapshots_list,
    snapshots_direct_search,
):
    """
    Generate a detailed report string based on the existence of a version in various locations.
    """
    if is_tombstoned:
        end_of_chain_description = "finding the deletion marker (tombstone). "
    else:
        end_of_chain_description = "reaching the end. "

    if snapshots_direct_search:
        if version_exists == VersionExistence.EXISTS_ONLY_IN_SNAPSHOTS:
            description = (
                f"***Snapshot {target} exists***."
                f"\n- You will need to traverse each of the {snapshots_count} snapshots to find "
                "the one you are looking for. "
            )
        else:
            description = (
                f"***Snapshot {target} does not exists***."
                f"\n- You will need to traverse all of the {snapshots_count} snapshots to verify "
                "that the one you are looking for does not exist. "
            )
    elif version_exists == VersionExistence.EXISTS_IN_VERSION_LIST:
        description = (
            f"***Version {target} exists***. \n- We need a total of {steps} steps in order to traverse the version list"
            f" until reaching version {target}. \n- Each step in the chain requires one I/O operation, so a large"
            " number of steps could lead to significant I/O and consequently affect performance. \n- If the number of"
            " steps is large, it is recommended to perform compaction to reduce the number of I/O operations required."
        )
    elif version_exists == VersionExistence.EXISTS_AS_COMPACTED_VERSION:
        description = (
            f"***Version {target} exists but has been compacted***. \n- You will traverse the version chain until you"
            f" find the node that represent the compacted versions. \n- It will require a total of {steps} extra I/O"
            " operations. \n- Then, you'll need to find the index keys for that version."
        )
    elif version_exists == VersionExistence.EXISTS_ONLY_IN_SNAPSHOTS:
        description = (
            f"***Version {target} has been deleted but is present in any of the snapshots***. \n- To find it, you will"
            f" first traverse the version chain, which requires a total of {steps} extra I/O operations to traverse the"
            " version chain until "
            + end_of_chain_description
        )
        description += (
            f"\n- Next, you will need to verify each of the {snapshots_count} snapshots to find "
            "which one(s) contain the version you are looking for. "
        )
        snapshots_list_str = ", ".join(f"**{name}**" for name in snapshots_list)
        description += f"\n- The list of snapshots contain the version is as follows: {snapshots_list_str}."
    else:
        version = "The specified as_of" if target is None else f"Version number {target}"
        description = (
            f"***{version} has been deleted or never existed for that symbol***. \n- It takes {steps} extra I/O"
            " operations to traverse the version chain until "
            + end_of_chain_description
        )
        description += (
            f"\n- There are {snapshots_count} snapshots present, so you would still need to check them for the version,"
            " which can be a costly operation."
            if snapshots_count > 0
            else "\n- There are no snapshots present, so no further checks are needed."
        )

    return description


def zigzag_layout(G, width=10):
    """
    Generate a zigzag layout for nodes in a graph.
    """
    pos = {}
    nodes = list(G.nodes())
    height = len(nodes) // width + 1
    for i, node in enumerate(nodes):
        x = i % width
        y = i // width
        if y % 2 == 1:  # If it's an odd row, reverse x
            x = width - 1 - x
        y = height - y  # Reverse the y value directly
        pos[node] = np.array([x, y])
    return pos


def add_graph_description(compacted_versions, version_exists, is_tombstoned):
    """
    Add a description of the graph to the plot.
    """
    graph_description = """
    \n In the graph below, each node represents a version from the version chain, and each edge represents a step from one version to the next. The nodes are colored based on their status: 
    \n- 'lightblue' indicates that the version is present in the version list and in the chain.
    \n- 'red' indicates that the version is deleted. 
    \n- 'green' represent versions that have been compacted. 
    \n\nThe version list starts from the latest version at the top left and continues until it either reaches the version we're looking for or finds that the version has been compacted or deleted.
    """
    display(Markdown(graph_description))

    plt.show()

    if compacted_versions and not (version_exists == VersionExistence.EXISTS_IN_VERSION_LIST or is_tombstoned):
        compaction_description = f"""
        \n **🟢 COMPACTED VERSIONS DETECTED 🟢**

        \n The node highlighted in green in the graph above represents a **compacted version**. 

        \n This node is a consolidation of multiple versions, improving the efficiency of data retrieval at the expense of some version history detail. 

        \n The compacted node specifically contains the following live versions:
        \n - {', '.join(map(str, compacted_versions))}

        \n Each of these versions is included in the compacted version, reducing the number of individual versions that need to be traversed.
        """
        display(Markdown(compaction_description))


def get_node_properties(current_key_version, seen_nodes, version_exists, is_last_node_and_exists_compacted):
    """
    Get the node colour and label for the node based on whether the version exists
    """
    if is_last_node_and_exists_compacted:
        return "lightgreen", f"C-{current_key_version}"
    elif not version_exists and current_key_version not in seen_nodes:
        return "red", f"T-{current_key_version}"
    else:
        return "lightblue", str(current_key_version)


def visualize_versions(
    parsed_versions,
    parsed_keys,
    version_exists,
    is_tombstoned,
    target,
    steps,
    compacted_versions,
):
    """
    Visualize the version chain using a directed graph layout.
    """
    G = nx.DiGraph()
    node_colors = []
    node_labels = {}

    # Track seen nodes
    seen_nodes = {}

    # Iterate over parsed_keys
    for i, key_obj in enumerate(parsed_keys):
        current_key_version = key_obj.version_id

        # Unique node name
        node_name = f"{current_key_version}_{i}"

        # Add the node to the graph
        G.add_node(node_name)

        # If we have compacted versions, it's guaranteed that the final version node will encompass them.
        is_last_node_and_exists_compacted = compacted_versions and i == len(parsed_keys) - 1

        version_exists = (
            current_key_version in parsed_versions and parsed_versions[current_key_version]["deleted"] == False
        )

        # Get node color and label
        node_color, node_label = get_node_properties(
            current_key_version, seen_nodes, version_exists, is_last_node_and_exists_compacted
        )

        node_colors.append(node_color)
        node_labels[node_name] = node_label

        # We need to mark the seen nodes to establish when a node is a tombstone and when it denotes the creation of a new version.
        seen_nodes[current_key_version] = True

        # Break loop if steps are 0 or current key version is target
        if steps == 0 or current_key_version == target:
            break

        # Add edge to next node
        if i < len(parsed_keys) - 1:
            next_node_name = f"{parsed_keys[i + 1].version_id}_{i + 1}"
            G.add_edge(node_name, next_node_name)

    plt.figure(figsize=(12, 8))
    pos = zigzag_layout(G)
    nx.draw(G, pos, labels=node_labels, node_size=1200, arrows=True, node_color=node_colors)
    add_graph_description(compacted_versions, version_exists, is_tombstoned)


def parse_version_list(versions):
    """
    Parse versions into a dictionary for easy access.
    """
    parsed_versions = {v["version"]: v for v in versions}
    return parsed_versions


def parse_version_keys_layer(keys):
    """
    Parse and sort keys based on creation timestamp.
    """
    parsed_keys = sorted(keys, key=lambda k: k.creation_ts, reverse=True)
    return parsed_keys


def get_target_version_from_timestamp(parsed_versions, timestamp):
    target_version = None
    latest_timestamp = None
    for version in parsed_versions.values():
        if version["date"] < timestamp and not version["deleted"]:
            if latest_timestamp is None or version["date"] > latest_timestamp:
                latest_timestamp = version["date"]
                target_version = version["version"]
    return target_version


def get_target_version(parsed_versions, parsed_keys, as_of):
    """
    Get the target version based on the 'as_of', or the latest version if 'as_of' is None.
    """
    if as_of is not None:
        if isinstance(as_of, supported_time_types):
            return get_target_version_from_timestamp(parsed_versions, as_of), False
        elif isinstance(as_of, str):
            return as_of, True
        else:
            return as_of, False

    # When the target version is None, we need to return the most recent 'alive' version, which is the
    # first non-tombstone one. The 'alive_versions' contains all the integers from the version list,
    # but we must always respect the order established by 'parsed_keys' (i.e., the actual physical version chain).
    # This is why we iterate across this strict order. Finally, we need to check the 'deleted' flag from
    # the version list, as we still need to exclude the versions that are present in the version list
    # solely because they are included in any snapshot.
    alive_versions = [key for key in parsed_versions.keys() if parsed_versions[key]["deleted"] == False]
    if alive_versions:
        return sorted(alive_versions, reverse=True)[0], False
    else:
        return None, False


def traverse_version_keys_layer(parsed_keys, target):
    """
    Traverse the version list to find the target version and the number of steps required to reach it.
    """
    steps = 0
    is_present = False
    for i, key in enumerate(parsed_keys):
        steps += 1
        if key.version_id == target:
            is_present = True
            break

    return steps - 1, is_present


def get_compacted_versions(parsed_versions, last_version_in_list):
    """
    Get all the compacted versions after the last node from the version list.
    """
    compacted_versions = []
    for version in parsed_versions.keys():
        if parsed_versions[version]["deleted"] == False and version <= last_version_in_list:
            compacted_versions.append(version)
    return compacted_versions


def determine_where_version_exists(
    present_in_version_list, parsed_versions, target, snapshots_direct_search, all_snapshots_lists
):
    """
    Determine the existence of the target version in various locations: version list, compacted versions, snapshots.
    """

    if snapshots_direct_search:
        if target in all_snapshots_lists:
            return VersionExistence.EXISTS_ONLY_IN_SNAPSHOTS
    elif target in parsed_versions.keys():
        if parsed_versions[target]["deleted"] == False:
            if present_in_version_list:
                return VersionExistence.EXISTS_IN_VERSION_LIST
            else:
                return VersionExistence.EXISTS_AS_COMPACTED_VERSION
        else:
            assert parsed_versions[target]["snapshots"]
            return VersionExistence.EXISTS_ONLY_IN_SNAPSHOTS

    return VersionExistence.NOT_EXISTS


def version_list_diagnosis(lib, symbol, as_of=None):
    """
    Perform a diagnosis on the version list of a given symbol in a library.

    This function diagnoses where a specific version of a symbol exists within a library.
    The version could be in the version list, compacted versions, or snapshots. It also checks if
    the version is tombstoned (deleted). The function generates a detailed report on the location
    and accessibility of the target version and visualizes the version chain using a graph.

    Parameters
    ----------
    lib : object
        The library object to perform the diagnosis on.
    symbol : str
        The symbol whose versions are to be diagnosed within the library.
    as_of : int, optional
        The specific version to diagnose. If not provided, the function will use the latest version.

    Returns
    -------
    None
        This function doesn't return anything.
        It generates a report and a graph visualization which are displayed but not returned.
    """
    lib, _ = check_and_adapt_library(lib)
    if lib is None:
        return
    if not check_symbol_exists(lib, symbol):
        return

    lib_tool = LibraryTool(lib._library)

    # get actual non-deleted versions
    versions = lib.list_versions(symbol)
    parsed_versions = parse_version_list(versions)

    # get actual version keys chain
    keys = lib_tool.find_keys_for_id(KeyType.VERSION, symbol)
    parsed_keys = parse_version_keys_layer(keys)

    # get specific version we are looking for
    target, snapshots_direct_search = get_target_version(parsed_versions, parsed_keys, as_of)

    # Even if the target we find it is a tombstone, we would stop here
    # we are just interested in the number of steps at this point
    steps, present_in_version_list = (
        traverse_version_keys_layer(parsed_keys, target) if not snapshots_direct_search else (0, False)
    )
    all_snapshots_lists = lib.list_snapshots()

    # Determine if the version exists in the version list, in the snapshots, or in a compacted version.
    version_exists = determine_where_version_exists(
        present_in_version_list, parsed_versions, target, snapshots_direct_search, all_snapshots_lists
    )
    is_tombstoned = present_in_version_list and version_exists in {
        VersionExistence.EXISTS_ONLY_IN_SNAPSHOTS,
        VersionExistence.NOT_EXISTS,
    }

    # get all the compacted versions after the last node from the version list
    compacted_versions = (
        get_compacted_versions(parsed_versions, parsed_keys[-1].version_id) if not snapshots_direct_search else []
    )

    # get the snapshots for the target version
    specific_version_snapshots_list = (
        parsed_versions[target]["snapshots"]
        if version_exists == VersionExistence.EXISTS_ONLY_IN_SNAPSHOTS and not snapshots_direct_search
        else None
    )

    # based on all the previous information, give a meaningful message to the user
    description = get_diagnosis_description(
        version_exists,
        is_tombstoned,
        target,
        steps,
        len(all_snapshots_lists),
        specific_version_snapshots_list,
        snapshots_direct_search,
    )
    display(Markdown(description))

    # Create and display graph for version list chain
    if not snapshots_direct_search:
        visualize_versions(
            parsed_versions,
            parsed_keys,
            version_exists,
            is_tombstoned,
            target,
            steps,
            compacted_versions,
        )
