import os
import shutil

# ======== Input and output paths ========
input_root = r"4Data desensitization"
output_root = r"5Data Name Change"

# ======== File Name Mapping Table ========
rename_map = {
    "AXDL": "Phase A current",
    "BXDL": "Phase B current",
    "CXDL": "Phase C current",
    "AXDY": "Phase A voltage",
    "BXDY": "Phase B voltage",
    "CXDY": "Phase C voltage",
    "AXWGGL": "Phase A reactive power",
    "BXWGGL": "Phase B reactive power",
    "CXWGGL": "Phase C reactive power",
    "AXYGGL": "Phase A active power",
    "BXYGGL": "Phase B active power",
    "CXYGGL": "Phase C active power",
    "YGGL": "Active power",
    "WGGL": "Reactive power"
}

def rename_indicator_file(filename: str) -> str:

    name, ext = os.path.splitext(filename)

    for key, new_name in rename_map.items():
        if name.startswith(key):
            return new_name + ext

    return filename


def copy_and_rename_files():
    for root, dirs, files in os.walk(input_root):

        relative_path = os.path.relpath(root, input_root)
        output_dir = os.path.join(output_root, relative_path)

        os.makedirs(output_dir, exist_ok=True)

        for file in files:
            input_file = os.path.join(root, file)

            new_filename = rename_indicator_file(file)
            output_file = os.path.join(output_dir, new_filename)

            shutil.copy2(input_file, output_file)

        for d in dirs:
            sub_output = os.path.join(output_dir, d)
            os.makedirs(sub_output, exist_ok=True)


if __name__ == "__main__":
    copy_and_rename_files()
    print("The file has been copied and renamed.")
