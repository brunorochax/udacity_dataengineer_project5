class CopyJson:
    copy_to_redshift = ("""
        COPY {}.{}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """)
