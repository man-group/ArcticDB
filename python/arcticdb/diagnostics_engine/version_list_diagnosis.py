import networkx as nx
import matplotlib.pyplot as plt
from arcticdb.toolbox.library_tool import LibraryTool, KeyType
import numpy as np
from IPython.display import display, Markdown
from .library_utils import check_and_adapt_library, check_symbol_exists


def get_diagnosis_description(
    exists_in_version_list,
    exists_as_compacted_version,
    exists_in_snaphots,
    is_tombstoned,
    target,
    steps,
    snapshots_count,
):
    """
    Generate a detailed report string based on the existence of a version in various locations.
    """
    if exists_in_version_list:
        description = (
            f"***Version {target} exists***. We need a total of {steps} extra I/O operations in"
            " order to traverse the version list until reaching version {target}. Each step in the chain requires one"
            " I/O operation, so a large number of steps could lead to significant I/O and consequently affect"
            " performance. If the number of steps is large, it is recommended to perform compaction to reduce the"
            " number of I/O operations required."
        )
    elif exists_as_compacted_version:
        description = (
            f"***Version {target} exists but has been compacted***. You will traverse the version chain until you find"
            f" the node that represent the compacted versions. It will require a total of {steps} extra I/O operations."
            " Then, you'll need to find the index keys for that version."
        )
    elif exists_in_snaphots:
        description = (
            f"***Version {target} has been deleted but is present in the snapshots***. To find it, you will first"
            f" traverse the version chain, which requires a total of {steps} extra I/O operations, until finding the"
            f" deletion marker (tombstone) or reaching the end. Then you will check all the {snapshots_count}"
            " snapshots, which can be a costly operation."
        )
    else:
        description = (
            f"***Version {target} has been deleted for that symbol***. It takes {steps} extra I/O operations to"
            " traverse the version chain until "
        )
        if is_tombstoned:
            description += "finding the deletion marker (tombstone)"
        else:
            description += "reaching the end. "
        description += (
            f"There are {snapshots_count} snapshots present, so you would still need to check them for the version,"
            " which can be a costly operation."
            if snapshots_count > 0
            else "There are no snapshots present, so no further checks are needed."
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


def add_graph_description(compacted_versions, exists_in_version_list, is_tombstoned):
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

    if compacted_versions and not (exists_in_version_list or is_tombstoned):
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
    exists_in_version_list,
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
    add_graph_description(compacted_versions, exists_in_version_list, is_tombstoned)


def parse_versions(versions):
    """
    Parse versions into a dictionary for easy access.
    """
    parsed_versions = {v["version"]: v for v in versions}
    return parsed_versions


def parse_keys(keys):
    """
    Parse and sort keys based on creation timestamp.
    """
    parsed_keys = sorted(keys, key=lambda k: k.creation_ts, reverse=True)
    return parsed_keys


def get_target_version(parsed_versions, parsed_keys, as_of):
    """
    Get the target version based on the 'as_of', or the latest version if 'as_of' is None.
    """
    if as_of is not None:
        return as_of

    alive_versions = parsed_versions.keys()
    for key in parsed_keys:
        if key in alive_versions and parsed_versions[key.version_id]["deleted"] == False:
            return key.version_id


def traverse_version_list(parsed_keys, target):
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


def determine_where_version_exists(present_in_version_list, parsed_versions, target):
    """
    Determine the existence of the target version in various locations: version list, compacted versions, snapshots.
    """
    exist_in_version_list = False
    exists_as_compacted_version = False
    exists_in_snaphots = False
    is_tombstoned = False

    if target in parsed_versions.keys():
        # if it exists either in the version list or compacted, we don't about about if also exists in snapshots
        if parsed_versions[target]["deleted"] == False:
            if present_in_version_list:
                exist_in_version_list = True
            else:
                exists_as_compacted_version = True
        else:
            if parsed_versions[target]["snapshots"]:
                exists_in_snaphots = True
    elif present_in_version_list:
        is_tombstoned = True
    return exist_in_version_list, exists_as_compacted_version, exists_in_snaphots, is_tombstoned


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
    lib = check_and_adapt_library(lib)
    if lib is None:
        return
    if not check_symbol_exists(lib, symbol):
        return

    lib_tool = LibraryTool(lib._library)

    # get non-deleted versions
    versions = lib.list_versions(symbol)
    parsed_versions = parse_versions(versions)

    # get actual version keys chain
    keys = lib_tool.find_keys_for_id(KeyType.VERSION, symbol)
    parsed_keys = parse_keys(keys)

    # get specific version we are looking for
    target = get_target_version(parsed_versions, parsed_keys, as_of)

    # Even if the target we find it is a tombstone, we would stop here
    # we are just interested in the number of steps at this point
    steps, present_in_version_list = traverse_version_list(parsed_keys, target)
    (
        exists_in_version_list,
        exists_as_compacted_version,
        exists_in_snaphots,
        is_tombstoned,
    ) = determine_where_version_exists(present_in_version_list, parsed_versions, target)

    # get all the compacted versions after the last node from the version list
    compacted_versions = get_compacted_versions(parsed_versions, parsed_keys[-1].version_id)

    # get the total number of snapshots
    snapshots_count = len(lib.list_snapshots())

    # based on all the previous information, give a meaningful message to the user
    description = get_diagnosis_description(
        exists_in_version_list,
        exists_as_compacted_version,
        exists_in_snaphots,
        is_tombstoned,
        target,
        steps,
        snapshots_count,
    )

    # Create and display graph for version list chain
    display(Markdown(description))
    visualize_versions(
        parsed_versions,
        parsed_keys,
        exists_in_version_list,
        exists_as_compacted_version,
        target,
        steps,
        compacted_versions,
    )
