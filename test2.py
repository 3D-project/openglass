
def main():
	filename = 'test.txt'
	entry = 'A' * 500
	for i in range(500000):
		with open(filename, 'a') as fh:
			fh.write(entry + '\n')


if __name__ == '__main__':
	main()


"""
python3 test2.py  4.03s user 3.02s system 99% cpu 7.118 total
python3 test2.py  3.52s user 3.32s system 99% cpu 6.900 total
python3 test2.py  3.89s user 3.25s system 99% cpu 7.210 total
"""
