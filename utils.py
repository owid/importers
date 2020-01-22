# import qgrid as qg

def write_file(file_path, content):
    with open(file_path, 'w') as f:
        f.write(content)

# def qgrid(df):
#     return qg.show_grid(df, show_toolbar=True)
