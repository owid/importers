from typing import List, Dict, Callable


class UnitConverter:
    def __init__(self, frm: str, to: str) -> None:
        self.frm = frm
        self.to = to

    @property
    def unit2fun(self) -> Dict[str, Dict[str, Callable[[float], float]]]:
        return {
            "ej": {"twh": ej2twh},
            "pj": {"twh": pj2twh},
            "twh": {"ej": twh2ej, "pj": twh2pj, "mtoe": twh2mtoe},
            "mtoe": {"twh": mtoe2twh},
        }

    @property
    def frm(self) -> str:
        return self._frm

    @frm.setter
    def frm(self, x: str) -> None:
        self._frm = x.strip().lower()

    @property
    def to(self) -> str:
        return self._to

    @to.setter
    def to(self, x: str) -> None:
        self._to = x.strip().lower()

    def get_converter(self) -> Callable[[float], float]:
        try:
            return self.unit2fun[self.frm][self.to]
        except KeyError:
            return None

    def can_convert(self) -> bool:
        return self.get_converter() is not None

    def convert(self, values: List[float]) -> List[float]:
        if not self.can_convert():
            raise NotImplementedError(
                f"{self.frm}->{self.to} conversion is not currently supported. `unit`"
                f" must be one of: {list(self.unit2fun.keys())}"
            )
        fun = self.get_converter()
        return [fun(v) for v in values]


def ej2twh(x: float) -> float:
    """Exajoules -> Terawatt-hours."""
    return x * (1 / 0.0036)


def twh2ej(x: float) -> float:
    """Terawatt-hours -> Exajoules."""
    return x * 0.0036


def pj2twh(x: float) -> float:
    """Petajoules -> Terawatt-hours."""
    return x * (1 / 3.6)


def twh2pj(x: float) -> float:
    """Terawatt-hours -> Petajoules."""
    return x * 3.6


def mtoe2twh(x: float) -> float:
    """Million tonnes of oil equivalent -> Terawatt-hours."""
    return x * 11.63


def twh2mtoe(x: float) -> float:
    """Terawatt-hours -> Million tonnes of oil equivalent."""
    return x * (1 / 11.63)
