mystr="Python is a great language\nlol\nhaha"
print mystr
fo = open("foo.txt", "w")
fo.write( mystr)
fo.write("xxxxxxxxx")

fo.close()