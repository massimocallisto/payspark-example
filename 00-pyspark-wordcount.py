findSpark package makes a Spark Context available in the code.


text_file = sc.textFile("README.md")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("output")

