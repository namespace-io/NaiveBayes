import shutil
import os

PPDIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
TRAIN_PATH = PPDIR + "/training"
TEST_PATH = PPDIR + "/test"


def file_classify(dir_path):
    for root, dirs, files in os.walk(dir_path):
        print(root)
        print(dirs)
        print(files)
        n = len(files)
        TRAIN_DIR = TRAIN_PATH + '/' + root
        TEST_DIR = TEST_PATH   + '/' + root
        # os.makedirs(TRAIN_DIR)
        # os.makedirs(TEST_DIR)
        if n > 3:
            for i in range(n):
                
                if(i < n * 0.7):
                    shutil.copy(root + '/' + files[i], TRAIN_DIR)
                else:
                    shutil.copy(root + '/' + files[i], TEST_DIR)


if __name__ == "__main__":
    
    if os.path.exists(TRAIN_PATH):
        os.removedirs(TRAIN_PATH)
    if os.path.exists(TEST_PATH):
        os.removedirs(TEST_PATH)
    file_classify('.')
