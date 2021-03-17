import io


def main():
	filename = 'test.txt'
	entry = 'A' * 500
	for i in range(500000):
		fh = io.open(filename, 'a')
		fh.write(entry + '\n')
		fh.close()


if __name__ == '__main__':
	main()


"""
python3 test3.py  3.65s user 3.13s system 98% cpu 6.851 total
python3 test3.py  3.75s user 3.26s system 99% cpu 7.085 total
python3 test3.py  3.78s user 3.16s system 98% cpu 7.023 total
"""
