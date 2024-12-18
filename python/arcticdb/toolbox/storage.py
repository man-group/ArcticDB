from typing import NamedTuple, Union, List

SymbolVersionsPair = NamedTuple("SymbolVersionsPair", [("id", Union[int, str]), ("versions", List[Union[int, str]])])
