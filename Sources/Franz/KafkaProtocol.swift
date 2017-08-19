//
//  KafkaProtocol.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

protocol KafkaType {
	init(data: inout Data)
	var data: Data { get }
	var dataLength: Int { get }
}

extension Data {
	mutating func take(first: Int) -> Data {
		if first > count {
			return Data()
		}
		let start = prefix(upTo: startIndex + first)
		removeFirst(first)
		return start
	}
}

extension KafkaType where Self: FixedWidthInteger {
	init(data: inout Data) {
		let dataLength = MemoryLayout<Self>.size
		self.init(bigEndian: data.take(first: dataLength).withUnsafeBytes { $0.pointee })
	}
	
	var dataLength: Int {
		return MemoryLayout<Self>.size
	}
	
	var data: Data {
		var bytes = Self(self.bigEndian)
		return Data(bytes: &bytes, count: MemoryLayout<Self>.size)
	}
}

extension UInt: KafkaType {}
extension UInt8: KafkaType {}
extension UInt16: KafkaType {}
extension UInt32: KafkaType {}
extension UInt64: KafkaType {}
extension Int: KafkaType {}
extension Int8: KafkaType {}
extension Int16: KafkaType {}
extension Int32: KafkaType {}
extension Int64: KafkaType {}

protocol KafkaVariableLengthType: KafkaType {
	associatedtype Length: FixedWidthInteger where Length: KafkaType
	
	var variableLengthData: Data { get }
	init(variableLengthData: Data)
}

extension KafkaVariableLengthType {
	
	init(data: inout Data) {
		let length = Int(Length(data: &data))
		if length == -1 {
			self.init(variableLengthData: Data())
			return
		}
		self.init(variableLengthData: data.take(first: length))
	}
	
	var lengthSize: Int {
		return MemoryLayout<Length>.size
	}
	
	var dataLength: Int {
		return variableLengthData.count + lengthSize
	}
	
	var data: Data {
		var finalData = Data(capacity: self.dataLength)
		finalData += Length(self.variableLengthData.count).data
		finalData += variableLengthData
		return finalData
	}
	
}

extension String: KafkaVariableLengthType {
	typealias Length = Int16
	
	init(variableLengthData: Data) {
		self.init(data: variableLengthData, encoding: .utf8)!
	}
	
	var variableLengthData: Data {
		return data(using: .utf8)!
	}
}

extension Optional where Wrapped == String {
	var dataLength: Int {
		return "".dataLength
	}
	
	var data: Data {
		return "".data
	}
}

extension Data: KafkaVariableLengthType {
	typealias Length = Int32
	
	init(variableLengthData: Data) {
		self = variableLengthData
	}
	
	var variableLengthData: Data {
		return self
	}
}

extension Optional where Wrapped == Data {
	var dataLength: Int {
		return Data().dataLength
	}
	
	var data: Data {
		return Data().data
	}
}

//TODO: Replace with Array extension conditional conformance is released
struct KafkaArray<T: KafkaType>: KafkaType, Collection {
	
	typealias Element = T
	typealias SubSequence = ArraySlice<T>
	typealias Index = Int
	
	var startIndex: Index {
		return values.startIndex
	}
	
	var endIndex: Index {
		return values.endIndex
	}
	
	subscript (position: Index) -> Iterator.Element {
		return values[position]
	}
	
	func index(after i: Index) -> Index {
		return values.index(after: i)
	}
	
	mutating func append(_ item: T) {
		values.append(item)
	}
	
	var values: [T]
	
	init(_ values: [T] = []) {
		self.values = values
	}
	
	init(data: inout Data) {
		let count = Int32(data: &data)
		
		values = [T]()
		
		for _ in 0..<count {
			values.append(T(data: &data))
		}
	}
	
	private let sizeDataLength = 4
	
	var dataLength: Int {
		return self.valuesDataLength + self.sizeDataLength
	}
	
	
	private var valuesDataLength: Int {
		var totalLength = 0
		
		for value in self.values {
			totalLength += value.dataLength
		}
		
		return totalLength
	}
	
	
	private var valuesData: Data {
		var valuesData = Data(capacity: self.valuesDataLength)
		
		for value in self.values {
			valuesData.append(value.data)
		}
		
		return valuesData
	}
	
	var data: Data {
		var finalData = Data(capacity: self.dataLength)
		
		let sizeData = Int32(self.values.count).data
		
		finalData.append(sizeData)
		finalData.append(self.valuesData)
		
		return finalData
	}
}
protocol KafkaMetadata: KafkaType {
    static var protocolType: GroupProtocol { get }
}
