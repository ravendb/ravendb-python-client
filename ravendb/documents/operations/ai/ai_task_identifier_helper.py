import unicodedata
from typing import List, Tuple


class AiTaskIdentifierHelper:
    """Helper class for validating and generating AI task identifiers."""

    @staticmethod
    def validate_identifier(identifier: str) -> Tuple[bool, List[str]]:
        """
        Validates an AI task identifier.

        Args:
            identifier: The identifier to validate.

        Returns:
            A tuple of (is_valid, errors). If valid, errors is an empty list.
        """
        errors: List[str] = []

        if not identifier or identifier.isspace():
            errors.append("Identifier cannot be empty or contain only whitespace;")
            return False, errors

        # Check that the string is already normalized (contains only a-z, 0-9 and hyphens)
        normalized = unicodedata.normalize("NFD", identifier)
        if identifier != normalized:
            errors.append("Identifier contains diacritical marks or non-ASCII characters;")

        # Check that there are no uppercase letters
        if any(c.isupper() for c in identifier):
            errors.append("Identifier contains uppercase letters;")

        # Check for invalid characters and collect them
        invalid_chars = set()
        for c in identifier:
            if not (("a" <= c <= "z") or ("0" <= c <= "9") or c == "-"):
                invalid_chars.add(c)

        if invalid_chars:
            chars_str = ", ".join(f"'{c}'" for c in sorted(invalid_chars))
            errors.append(
                f"Identifier contains invalid characters: {chars_str}. "
                f"Only lowercase letters (a-z), numbers (0-9) and hyphens (-) are allowed."
            )

        # Check that there are no consecutive hyphens
        if "--" in identifier:
            errors.append("Identifier contains consecutive hyphens;")

        # Check that the string does not end with a hyphen
        if identifier.endswith("-"):
            errors.append("Identifier ends with a hyphen;")

        return len(errors) == 0, errors

    @staticmethod
    def generate_identifier(input_str: str) -> str:
        """
        Generates a valid identifier from an input string.

        Args:
            input_str: The input string to convert to an identifier.

        Returns:
            A valid identifier string, or a default identifier if input is empty.
        """
        if not input_str or input_str.isspace():
            return None

        result = []
        last_was_hyphen = False

        # First normalize to FormD to separate letters from their diacritics
        normalized = unicodedata.normalize("NFD", input_str)

        for c in normalized:
            # Check if this is a letter that needs to be preserved
            if "a" <= c <= "z" or "0" <= c <= "9":
                result.append(c)
                last_was_hyphen = False
            elif "A" <= c <= "Z":
                result.append(c.lower())
                last_was_hyphen = False
            elif not last_was_hyphen and len(result) > 0:
                # Add hyphen for any other character
                result.append("-")
                last_was_hyphen = True

        # Trim any trailing hyphens
        final_result = "".join(result).rstrip("-")

        # Ensure we have at least one character
        return final_result if final_result else "AiConnectionStringIdentifier"
