# Example of python script to use on mongo-converter

def parser_field(field, row=None, configuration=None,
                 mongo_column=None,
                 oracleConnection=None,
                 operator=None):

    print("Handle field", field, ' of column', mongo_column)

    # skip operator
    # if op:
    #    op.skip_column = True

    ans = '%s changed' % field

    # Return value to store
    return ans

# vim: ts=4 sw=4 expandtab