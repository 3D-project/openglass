import io


def main():
	filename = 'test.txt'
	entry = 'A' * 500
	for i in range(500000):
		fh = io.open(filename, 'ab')
		fh.write(bytearray(entry + '\n', 'utf-8'))
		fh.close()


if __name__ == '__main__':
	main()


"""
python3 test4.py  1.87s user 2.12s system 98% cpu 4.044 total
python3 test4.py  1.65s user 2.22s system 98% cpu 3.933 total
python3 test4.py  1.77s user 2.33s system 98% cpu 4.157 total
"""
