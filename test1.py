
def main():
	filename = 'test.txt'
	entry = 'A' * 500
	for i in range(500000):
		fh = open(filename, 'a')
		fh.write(entry + '\n')
		fh.close()


if __name__ == '__main__':
	main()


"""
python3 test1.py  3.62s user 3.09s system 99% cpu 6.774 total
python3 test1.py  3.39s user 2.99s system 98% cpu 6.461 total
python3 test1.py  3.61s user 3.13s system 98% cpu 6.810 total
"""
