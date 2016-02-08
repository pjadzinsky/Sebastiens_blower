CAGE_DECORATION_STRING = 'CAGE'
SLAB_DECORATION_STRING = ''
SUBJECT_DECORATION_STRING = 'SUBJECT'

def get_slab_source(slab_mac_address_forwards):
    return slab_mac_address_forwards

def get_cage_source(cage_id):
    return decorate_source_name(cage_id, CAGE_DECORATION_STRING)

def get_subject_source(subject_id):
    return decorate_source_name(subject_id, SUBJECT_DECORATION_STRING)

#standard way to decorate source names so they're unique when metric sources of different types share IDs.
#For example, if there is a cage and a subject, both with IDs of 200, we want their metrics filed separately
#Cage 200's and Subject 200's metrics will have a source of 'CAGE.200.C' and 'SUBJECT.200.S' respectively.
def decorate_source_name(id, source_type):
    #Tack on the first letter of the source type at the end of the result, so that metrics of the same source-type
    #are adjacent on S3 (which files metrics by reversed source_type)
    return '.'.join((source_type, str(id), source_type[0]))


def is_cage_source(source):
    '''
    Determines if a JobMessage's source is a cage source (i.e. created with get_cage_source)

    :param payload: message payload
    :return:
    '''
    if 'cage' in source.lower():
        return True

    return False

