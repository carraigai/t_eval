import pytest
from src.task1 import produce_summary
import tempfile
import os


def test_produce_summary():

    filename = "fixtures/groceries_mini.csv"

    with tempfile.TemporaryDirectory() as tmp:
        unique_products_output_path = os.path.join(tmp, "dud1.txt")
        product_count_output_path = os.path.join(tmp, "dud2.txt")
        top_products_output_path = os.path.join(tmp, "dud3.txt")
        produce_summary(filename, top_products_output_path,
                        unique_products_output_path,
                        product_count_output_path, "local[1]")

        # test unique products
        with open(unique_products_output_path, "r") as fp:
            data = fp.read().splitlines()
            assert len(data) == 10
            assert data[0] == "fruit"
            assert data[1] == "margarine"
            assert data[2] == "yogurt"
            assert data[3] == "coffee"
            assert data[4] == "whole milk"
            assert data[5] == "rice"
            assert data[6] == "cream cheese"
            assert data[7] == "long life bakery product"
            assert data[8] == "butter"
            assert data[9] == "abrasive cleaner"


        # test product count
        with open(product_count_output_path, "r") as fp:
            data = fp.read().splitlines()
            assert len(data) == 2
            assert data[0] == "Count:"
            assert data[1] == "18"


        # test top5 products
        with open(top_products_output_path, "r") as fp:
            data = fp.read().splitlines()
            assert len(data) == 5
            assert data[0] == "('yogurt', 4)"
            assert data[1] == "('whole milk', 3)"
            assert data[2] == "('fruit', 2)"
            assert data[3] == "('rice', 2)"
            assert data[4] == "('cream cheese', 2)"

