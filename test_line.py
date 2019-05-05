from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        line_data = line.split(',')
        movie_id = line_data[1]
        rating = line_data[2]
        yield movie_id, 
        # yield "chars", len(line)
        yield "words", len(line.split())
        # yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
