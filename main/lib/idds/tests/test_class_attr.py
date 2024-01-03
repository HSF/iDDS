class ExampleClass(object):
    class_attr = 0

    def __init__(self, instance_attr):
        self.instance_attr = instance_attr


if __name__ == '__main__':

    foo = ExampleClass(1)
    bar = ExampleClass(2)

    print(foo.instance_attr)
    print(foo.class_attr)

    print(bar.instance_attr)
    print(bar.class_attr)

    print(ExampleClass.class_attr)

    foo.class_attr = 100

    print(foo.instance_attr)
    print(foo.class_attr)

    print(bar.instance_attr)
    print(bar.class_attr)

    print(ExampleClass.class_attr)

    ExampleClass.class_attr = 100

    print(foo.instance_attr)
    print(foo.class_attr)

    print(bar.instance_attr)
    print(bar.class_attr)

    print(ExampleClass.class_attr)
