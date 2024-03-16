import os

def search_dir(directory_path,extension):
    file_paths = []
    if os.path.exists(directory_path):
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.endswith(extension):
                    full_path = os.path.join(root, file).replace('\\', '/')
                    file_paths.append(full_path)
    return file_paths

fpaths = search_dir("C:\\SofascoreAcademy2024\\data-science-academy-homework\\HW2\\raw_data\\february",".csv")

for p in fpaths:
   print(p)