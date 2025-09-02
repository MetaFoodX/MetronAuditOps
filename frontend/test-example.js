// Example JavaScript file to test pre-commit hooks

const exampleFunction = (name, age = null) => {
  if (age === null) {
    return `Hello ${name}!`;
  }
  return `Hello ${name}, you are ${age} years old!`;
};

class ExampleClass {
  constructor(data) {
    this.data = data;
  }

  getSum() {
    return this.data.reduce((sum, num) => sum + num, 0);
  }

  getMean() {
    return this.getSum() / this.data.length;
  }
}

// Example usage
const example = new ExampleClass([1, 2, 3, 4, 5]);
console.log(example.getSum());
console.log(example.getMean());
console.log(exampleFunction("World"));

export { exampleFunction, ExampleClass };
