from typing import NamedTuple, Union, List

SymbolVersionsPair = NamedTuple("SymbolVersionsPair", [("id", Union[int, bytes]), ("versions", List[Union[int, bytes]])])
