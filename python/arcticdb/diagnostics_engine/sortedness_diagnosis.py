from IPython.display import display, Markdown
import matplotlib.pyplot as plt
import networkx as nx
from .library_utils import check_and_adapt_library, check_symbol_exists


def construct_state_machine(highlight_node):
    """
    Constructs a state machine based on the sorted state.
    """
    # Create a directed graph
    G = nx.DiGraph()

    # Define the nodes and positions
    nodes_positions = {"UNKNOWN": (0, 1), "ASCENDING": (-1, 0), "DESCENDING": (1, 0), "UNSORTED": (0, -1)}

    # Add nodes to the graph
    G.add_nodes_from(nodes_positions.keys())

    # Define the edges and their labels
    edges = [
        ("UNKNOWN", "UNSORTED", "UNSORTED"),
        ("ASCENDING", "UNKNOWN", "UNKNOWN"),
        ("ASCENDING", "UNSORTED", "UNSORTED OR DESCENDING"),
        ("DESCENDING", "UNKNOWN", "UNKNOWN"),
        ("DESCENDING", "UNSORTED", "UNSORTED OR ASCENDING"),
        ("UNSORTED", "UNSORTED", "ANY"),
    ]

    # Add edges to the graph
    for edge in edges:
        G.add_edge(edge[0], edge[1], label=edge[2])

    # Define color map
    color_map = ["yellow" if node == highlight_node else "lightblue" for node in G.nodes]

    # Draw the graph
    plt.figure(figsize=(8, 8))
    nx.draw(
        G,
        nodes_positions,
        labels={node: node for node in G.nodes()},
        node_color=color_map,
        with_labels=True,
        node_size=5000,
        font_size=12,
        font_weight="bold",
        connectionstyle="arc3,rad=0.1",
    )

    # Draw edge labels
    edge_labels = nx.get_edge_attributes(G, "label")
    nx.draw_networkx_edge_labels(G, nodes_positions, edge_labels=edge_labels)

    # Show the graph
    plt.show()


def get_append_description(sorted_state):
    """
    Provides a description for the append method based on the current sorted state.
    """
    if sorted_state == "ASCENDING":
        return (
            "\n- **Append method**: The existing data is sorted in an ASCENDING order. If the new data that you're"
            " trying to append is not also sorted in an ASCENDING order, and the 'validate_index' flag is set to True,"
            " a sorting exception (E_UNSORTED_DATA) will be raised. However, this condition only applies if the data is"
            " time-series data. If it's not time-series data, no sorting exception will be raised regardless of the"
            " data's order."
        )
    else:
        return (
            "\n- **Append method**: If either the existing dataframe or the new input is not explicitly ASCENDING,"
            " and the validate_index flag is set to True, a sorting exception"
            " (E_UNSORTED_DATA) is raised. However, this condition only applies if the data is"
            " time-series data. If it's not time-series data, no sorting exception will be raised regardless of the"
            " data's order."
        )


def get_write_description():
    """
    Provides a description for the write method.
    """
    return (
        "\n- **Write method**: If the input data is not explicitly ASCENDING and the validate_index flag is set to"
        " True, a sorting exception (E_UNSORTED_DATA) is raised. However, this condition only applies if the data is"
        " time-series data. If it's not time-series data, no sorting exception will be raised regardless of the data's"
        " order."
    )


def get_update_description(sorted_state):
    """
    Provides a description for the update method based on the current sorted state.
    """
    if sorted_state not in ["ASCENDING", "UNKNOWN"]:
        return (
            "\n- **Update method**: Your data is not in an ASCENDING or UNKNOWN state. Therefore, a sorting exception"
            " (E_UNSORTED_DATA) will be raised if we do an update. Note that the Update method does not have a"
            " user-settable validate_index flag."
        )
    else:
        return (
            "\n- **Update method**: Your data is either in an ASCENDING or UNKNOWN state. Therefore, a sorting"
            " exception ('E_UNSORTED_DATA') will not be raised as long as the input data remains either in an ASCENDING"
            " or UNKNOWN state and is also a time series. Please note that the 'Update' method does not have a"
            " user-settable validate_index flag."
        )


def get_read_description(sorted_state):
    """
    Provides a description for the read method based on the current sorted state.
    """
    if sorted_state != "ASCENDING" and sorted_state != "UNKNOWN":
        return (
            "\n- **Read method with date range filter**: A sorting exception (E_UNSORTED_DATA) will be raised because"
            " your data is explicitly UNSORTED or DESCENDING. This method does not have a user-settable validate_index"
            " flag."
        )
    else:
        return (
            "\n- **Read method with date range filter**: Your data is either UNKNOWN or ASCENDING so no sorting"
            " exception will be raised."
        )


def sortedness_diagnosis(lib, symbol, as_of=None):
    """
    Checks the sortedness of data in ArcticDB. It provides a description of the outcomes for different methods (append, write, update, read)
    based on the sorting state, and constructs a state machine diagram illustrating the process of appending or updating dataframes.

    Parameters:
    ----------
    lib : str
        The library where the data is stored.
    symbol : str
        The symbol of the data to be checked.
    as_of : str, optional
        The version of the data, if applicable. By default, None.

    Returns:
    -------
    None
    """
    lib = check_and_adapt_library(lib)
    if lib is None:
        return

    if not check_symbol_exists(lib, symbol, as_of):
        return

    info = lib.get_info(symbol, as_of)

    description = f"""
    \n Your symbol's sorting status in ArcticDB is currently {info['sorted']}.
        \n- The graph below is a state machine diagram that represents the process of appending or updating dataframes for your specific symbol and version. 
        \n- If a transition isn't depicted for an event, it signifies that the state doesn't change for that event.
        \n- The current state of your data is highlighted in yellow in the node diagram.
    """
    display(Markdown(description))

    construct_state_machine(info["sorted"])

    sorted_state = info["sorted"]

    append_description = get_append_description(sorted_state)
    write_description = get_write_description()
    update_description = get_update_description(sorted_state)
    read_description = get_read_description(sorted_state)

    personalized_description = f"""
    \n Based on the current sortedness state ({sorted_state}) of your data, here are the implications for various methods:

    {append_description}
    {write_description}
    {update_description}
    {read_description}
    """
    display(Markdown(personalized_description))
