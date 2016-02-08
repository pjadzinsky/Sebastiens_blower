import re


class MetricFile(object):
    def __init__(self, file, full_path=None, tags_to_get=None):
        """

        :param file: file handle
        :param full_path: file path (for better error messages)
        :param tags_to_get: get_value returns only tags with keys contained in this iterable (e.g. ['version']).
                    None will retrieve all tags listed on each line (somewhat confusingly).
                    {} will retrieve no tags
        :return:
        """
        self.file = file
        self.read_line = 0
        self.full_path = full_path
        self.tags_to_get = frozenset(tags_to_get) if (tags_to_get is not None) else None

    def put_value(self, name, timestamp, value, **tags):
        #appends a value to the metrics file (must be opened for write)
        try:
            float(value)
        except ValueError:
            print ("Metric value '%s' is not a float - adding 'type=string' tag" % (value))
            #could not convert value to float so save it as a string
            #add a "type=string' tag
            tags = dict(tags)
            tags['type'] = 'string'

        line = "put %s %d %s" % (name, int(timestamp), value)
        for key in tags.iterkeys():
            line += " %s=%s" % (key,tags[key])
        line+='\n'
        self.file.write(line)

    def get_value(self):
        # reads a line and returns a tuple:
        # (name, timestamp, value, tags_dict)
        # or None if end of file
        line = self.file.readline()
        self.read_line += 1
        if not line:
            return None
        groups = line.split()
        if (groups[0] != 'put'):
            raise ValueError("Unparsable metric file line (must start with 'put') %d: '%s'" % (self.read_line, line))
        groups = groups[1::]
        if (len(groups) < 3):
            raise ValueError("Unparsable metric file line %d: '%s'" % (self.read_line, line))

        name = groups[0]
        try:
            timestamp = int(groups[1])
        except ValueError as e:
            raise ValueError("Unparsable timestamp '%s' in metric file line %d: '%s'" % (groups[1], self.read_line, line))
        try:
            value = float(groups[2])
        except ValueError as e:
            print ("Non-float metric value '%s' in metric file line %d. Treating as a string: '%s'" % (groups[2], self.read_line, line))
            value = groups[2]

        tags = {}
        if self.tags_to_get is None or self.tags_to_get != {}:  #skip tag processing (and validation!!) if caller doesn't care about tags
            for tag in groups[3::]:
                split = tag.split('=')
                if len(split) != 2:
                    raise ValueError("Invalid tag '%s' in metric file line %d: '%s'" % (tag, self.read_line, line))
                if self.tags_to_get is None or (split[0] in self.tags_to_get):
                    tags[split[0]] = split[1]

        return (name, timestamp, value, tags)
