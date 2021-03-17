import os
import io


def main():
	filename = 'test.txt'
	entry = 'A' * 500
	for i in range(500000):
		fh = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT)
		os.write(fh, entry.encode('utf-8') + b'\n')
		os.close(fh)


if __name__ == '__main__':
	main()


"""
python3 test6.py  0.50s user 1.28s system 96% cpu 1.850 total
python3 test6.py  0.61s user 1.20s system 96% cpu 1.875 total
python3 test6.py  0.56s user 1.29s system 96% cpu 1.909 total
"""
