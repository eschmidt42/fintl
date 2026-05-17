"""Input validators for the search command filter fields."""

from dateutil.parser import parse
from textual.validation import ValidationResult, Validator


class DateValidator(Validator):
    """Validator that accepts empty strings or parseable date strings."""

    def validate(self, value: str) -> ValidationResult:
        """Return success if value is empty or a valid date string."""
        if not value:
            return self.success()
        try:
            parse(value)
            return self.success()
        except ValueError:
            return self.failure("Invalid date")


class AmountValidator(Validator):
    """Validator that accepts empty strings or numeric amount strings."""

    def validate(self, value: str) -> ValidationResult:
        """Return success if value is empty or convertible to a float."""
        if not value:
            return self.success()
        try:
            float(value)
            return self.success()
        except ValueError:
            return self.failure("Must be a number")
