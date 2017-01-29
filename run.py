from pelicansage.sagecell import main

if __name__ == '__main__':
    import pelican
    import sys

    sys.argv = ['/home/projects/pelican/nelsonbrown.net/content/']
    pelican.main()