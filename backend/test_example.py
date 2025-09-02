"""Example test file to verify pre-commit hooks are working."""

import numpy as np
import os
import pandas as pd
import sys
from fastapi import FastAPI
from typing import List, Optional


def example_function(name: str, age: Optional[int] = None) -> str:
    """Example function with type hints.

    Args:
        name: The person's name
        age: The person's age (optional)

    Returns:
        A greeting string
    """
    if age is None:
        return f"Hello {name}!"
    return f"Hello {name}, you are {age} years old!"


class ExampleClass:
    """Example class to test code quality tools."""

    def __init__(self, data: List[int]):
        self.data = data

    def get_sum(self) -> int:
        """Calculate the sum of the data."""
        return sum(self.data)

    def get_mean(self) -> float:
        """Calculate the mean of the data."""
        return np.mean(self.data)


# Example usage
if __name__ == "__main__":
    app = FastAPI()
    example = ExampleClass([1, 2, 3, 4, 5])
    print(example.get_sum())
    print(example.get_mean())
    print(example_function("World"))
