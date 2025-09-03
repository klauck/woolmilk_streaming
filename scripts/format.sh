echo "===== Sorting imports ====="

isort --trailing-comma --line-width 90 --multi-line 3 woolmilk/
isort --trailing-comma --line-width 90 --multi-line 3 playground/py_arrow_flight/

echo ""
echo "===== Formatting via black ====="

black --line-length 90 woolmilk/
black --line-length 90 playground/py_arrow_flight/
