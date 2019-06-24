class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def move(self):
        print("I am moving")


mum = Person("Martina", 49)
dad = Person("Aggay", 60)


print(mum.name)
print(dad.name)

mum.move()


