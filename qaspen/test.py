class Test:
    def __set_name__(self, owner, name: str):
        print(owner)
        self.name = name


class Sasa:
    wow = Test()


print(Sasa().wow.name)
