# Franz

[![Build Status](https://travis-ci.org/kellanburket/franz.svg?branch=master)](https://travis-ci.org/kellanburket/franz)
[![Version](https://img.shields.io/cocoapods/v/Franz.svg?style=flat)](http://cocoapods.org/pods/Franz)
[![License](https://img.shields.io/cocoapods/l/Franz.svg?style=flat)](http://cocoapods.org/pods/Franz)
[![Platform](https://img.shields.io/cocoapods/p/Franz.svg?style=flat)](http://cocoapods.org/pods/Franz)


Franz is an Apache Kafka 0.9.0 client for iOS and macOS.

## Usage

```swift
import Franz

let cluster = Cluster(brokers: [("localhost", 9092)], clientId: "FranzExample")

let consumer = cluster.getConsumer(topics: ["test"], groupId: "group")
consumer.listen { message in
	print(String(data: message.value, encoding: .utf8)!)
}

cluster.sendMessage("test", message: "Hello world!")
```

You can view the [documentation here](//kellanburket.github.com/franz).

The current release of Franz should be considered beta. It is not necessarily ready for production code.

The repo has example projects for CocoaPods and Swift Package Manager. 

## Installation

### [CocoaPods](http://cocoapods.org)

Add the following line to your `Podfile`:

```ruby
pod "Franz"
```

### Swift Package Manager

Add the following dependency to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/kellanburket/franz.git", from: "1.0.0"),
],
targets: [
    .target(name: "MyTarget", dependencies: ["Franz"])
]
```

## Author

Kellan Cummings

Luke Lau

## License

Franz is available under the MIT license. See the LICENSE file for more info.
