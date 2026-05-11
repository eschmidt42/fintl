from dateutil.parser import parse
from textual.validation import ValidationResult, Validator


class DateValidator(Validator):
    def validate(self, value: str) -> ValidationResult:
        if not value:
            return self.success()
        try:
            parse(value)
            return self.success()
        except ValueError:
            return self.failure("Invalid date")


class AmountValidator(Validator):
    def validate(self, value: str) -> ValidationResult:
        if not value:
            return self.success()
        try:
            float(value)
            return self.success()
        except ValueError:
            return self.failure("Must be a number")
