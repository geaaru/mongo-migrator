# Example of python script to use on mongo-converter

def parser_field(field, row=None, configuration=None,
                 mongo_column=None,
                 oracleConnection=None,
                 mongoClient=None,
                 context=None,
                 operator=None):

    print("Handle field", field, ' of column', mongo_column)

    # skip operator
    # if op:
    #    op.skip_column = True

    ans = '%s changed' % field

    # Store context field for retrieve it on
    # next raw
    context['my_reusable_obj'] = 'XXXX'

    # Return value to store
    return ans

# vim: ts=4 sw=4 expandtab