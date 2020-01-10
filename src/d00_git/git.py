def git(message):
    '''Function adds, commits with message, and pushes to github'''
    !git add .
    !git commit -m f'{message}'
    !git push origin master