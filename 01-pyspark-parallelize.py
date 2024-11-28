data = [1, 2, 3, 4, 5,6,7,8,9,10]
nums = sc.parallelize(data, 10)
mapped_nums = nums.map(lambda x: x*x)
mapped_nums.collect()
