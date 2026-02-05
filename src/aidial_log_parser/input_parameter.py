from typing import Optional

import click


class InputParameter(click.ParamType):
    name = "input_parameter"

    class Unset:
        """Unset sentinel value for click options.
        Used to distinguish between default value and None value, provided by user.
        """

        pass

    def convert(
        self,
        value: str | Unset,
        param: Optional[click.Parameter],
        ctx: Optional[click.Context],
    ) -> str | None | Unset:
        if isinstance(value, str) and value.lower() == "none":
            return None
        return value

    @staticmethod
    def create_params_kwargs(**kwargs) -> dict:
        """Create kwargs dict for the parameters, excluding Unset values."""
        return {
            k: v for k, v in kwargs.items() if not isinstance(v, InputParameter.Unset)
        }
