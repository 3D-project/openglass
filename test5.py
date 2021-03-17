import io
# import os


def main():
	filename = 'test.txt'
	entry = 'A' * 500
	for i in range(500000):
		# size = os.path.getsize(filename)
		fh = io.open(filename, 'rb+')
		fh.seek(0, 2)
		fh.write(bytearray(entry + '\n', 'utf-8'))
		fh.close()


if __name__ == '__main__':
	main()


"""
python3 test5.py  2.27s user 2.58s system 98% cpu 4.910 total
python3 test5.py  2.09s user 2.75s system 98% cpu 4.900 total
python3 test5.py  2.13s user 2.66s system 98% cpu 4.854 total
"""
