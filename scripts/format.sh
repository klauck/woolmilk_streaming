echo "===== Sorting imports ====="

isort --trailing-comma --line-width 90 --multi-line 3 py_arrow_flight/
isort --trailing-comma --line-width 90 --multi-line 3 py_arrow_flight-example/

echo ""
echo "===== Formatting via black ====="

black --line-length 90 py_arrow_flight/
black --line-length 90 py_arrow_flight-example/
