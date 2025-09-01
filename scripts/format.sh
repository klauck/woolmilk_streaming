echo "===== Sorting imports ====="

isort --trailing-comma --line-width 90 --multi-line 3 py_arrow_flight-example/
isort --trailing-comma --line-width 90 --multi-line 3 py_arrow_flight/

echo ""
echo "===== Formatting via black ====="

black --line-length 90 py_arrow_flight-example/
black --line-length 90 py_arrow_flight/
