import os


def rmov():
    rmv = []
    for root, dir, files in os.walk(r"\\fintechdata\SecOriData\10QK_20190531"):
        for file in files:
            file_date = int(file.split('_')[0])
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)

            if file_date > 20190531 or file_size < 500:
                rmv.append(file_path)
    for i in rmv:
        os.remove(i)
        print(i)
if __name__ == '__main__':
    rmov()