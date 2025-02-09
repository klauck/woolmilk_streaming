import os
import subprocess
import re
import json
import random
import string
import sys

def random_text(max_length=25):
    length = random.randint(1, max_length)
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <number>")
        sys.exit(1)
    n_value = sys.argv[1]

    current_file_path = os.path.abspath(__file__)

    # Execute the bash command "nexmark -n 1000 --no-wait"
    try:
        result = subprocess.run(
            ["nexmark", "-n", n_value, "--no-wait"],
            capture_output=True,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        return

    output_text = result.stdout

    pattern = r'"category":\s*(\d+)'
    category_numbers = re.findall(pattern, output_text)

    unique_category_ids = {int(num) for num in category_numbers}

    for cat_id in sorted(unique_category_ids):
        json_obj = {"Category": {"id": cat_id, "name": random_text(25)}}
        output_text += json.dumps(json_obj) + "\n"


    output_path = os.path.join(os.path.dirname(current_file_path), "../data/data.txt")

    with open(output_path, "w") as f:
        f.write(output_text)

    print(f"Data written to {output_path}")

if __name__ == "__main__":
    main()
