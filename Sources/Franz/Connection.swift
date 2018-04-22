//
//  Connection.swift
//  Franz
//
//  Created by Kellan Cummings on 1/13/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

enum ConnectionError: Error {
    case unableToOpenConnection
    case invalidIpAddress
    case cannotProcessMessageData
    case invalidCorrelationId
    case noResponse
    case zeroLengthResponse
    case partialResponse(size: Int32)
    case inputStreamUnavailable
    case inputStreamError
    case unableToFindInputStream
    case outputStreamUnavailable
    case outputStreamError(error: String)
    case outputStreamHasEnded
    case outputStreamClosed
    case unableToWriteBytes
    case bytesNoLongerAvailable
}


extension Stream.Event {
	var description: String {
		switch self {
		case []:
			return "None"
		case .openCompleted:
			return "Open Completed"
		case .hasBytesAvailable:
			return "Has Bytes Available"
		case .hasSpaceAvailable:
			return "Has Space Available"
		case .errorOccurred:
			return "Error Occurred"
		case .endEncountered:
			return "End Encountered"
		default:
			return ""
		}
	}
}

extension Stream.Status {
	var description: String {
		switch self {
		case .notOpen:
			return "Not Open"
		case .opening:
			return "Opening"
		case .open:
			return "Open"
		case .reading:
			return "Reading"
		case .writing:
			return "Writing"
		case .atEnd:
			return  "End"
		case .closed:
			return "Closed"
		case .error:
			return "Error"
		}
	}
}

class Connection: NSObject, StreamDelegate {
	
	enum AuthenticationError: Error {
		case unsupportedMechanism(supportedMechanisms: [String])
		case authenticationFailed
	}
    
    private var ipv4: String

    var apiVersion: ApiVersion {
        return ApiVersion.defaultVersion
    }

    private var _requestCallbacks = [Int32: ((Data) -> Void)]()
    
    private var clientId: String
    
    private var readStream: Unmanaged<CFReadStream>?
    private var writeStream: Unmanaged<CFWriteStream>?

    private var inputStream: InputStream?
    private var outputStream: OutputStream?
    
    private var port: Int32
    
    private let responseLengthSize: Int32 = 4
    private let responseCorrelationIdSize: Int32 = 4

    private var _inputStreamQueue: DispatchQueue
    private var _outputStreamQueue: DispatchQueue
    private var _writeRequestBlocks = [()->()]()
	
	struct Config {
		let ipv4: String
		let port: Int32
		let clientId: String
		let authentication: Cluster.Authentication
	}
    
	init(config: Config) throws {
        self.ipv4 = config.ipv4
        self.clientId = config.clientId
        self.port = config.port

        _inputStreamQueue = DispatchQueue(
            label: "\(self.ipv4).\(self.port).input.stream.franz", attributes: []
        )

        _outputStreamQueue = DispatchQueue(
            label: "\(self.ipv4).\(self.port).output.stream.franz", attributes: []
        )

        super.init()

        CFStreamCreatePairWithSocketToHost(
            kCFAllocatorDefault,
            ipv4 as CFString,
            UInt32(port),
            &readStream,
            &writeStream
        )

        inputStream = readStream?.takeUnretainedValue()
        outputStream = writeStream?.takeUnretainedValue()
		
		DispatchQueue(label: "FranzConnection").async {
			self.inputStream?.delegate = self
			self.inputStream?.schedule(
				in: RunLoop.current,
				forMode: RunLoopMode.defaultRunLoopMode
			)
			
			self.outputStream?.delegate = self
			self.outputStream?.schedule(
				in: RunLoop.current,
				forMode: RunLoopMode.defaultRunLoopMode
			)
			
			self.inputStream?.open()
			self.outputStream?.open()
			
			RunLoop.current.run()
		}
		
		// authenticate
		if let mechanism = config.authentication.mechanism {
			let handshakeRequest = SaslHandshakeRequest(mechanism: mechanism.kafkaLabel)
			let response = self.writeBlocking(handshakeRequest)
			
			guard response.errorCode == 0 else {
				throw AuthenticationError.unsupportedMechanism(supportedMechanisms: response.enabledMechanisms)
			}
			
			if !mechanism.authenticate(connection: self) {
				throw AuthenticationError.authenticationFailed
			}
		}

    }
    
    private func read(_ timeout: Double = 3000) {
        _inputStreamQueue.async {
            if let inputStream = self.inputStream {
                do {
                    let (size, correlationId) = try self.getMessageMetadata()
                    var bytes = [UInt8]()
                    let startTime = Date().timeIntervalSince1970
                    while bytes.count < Int(size) {
                        
                        if inputStream.hasBytesAvailable {
                            var buffer = [UInt8](repeating: 0, count: Int(size))
                            let bytesInBuffer = inputStream.read(&buffer, maxLength: Int(size))
                            bytes += buffer.prefix(upTo: Int(bytesInBuffer))
                        }
                        
                        let currentTime = Date().timeIntervalSince1970
                        let timeDelta = (currentTime - startTime) * 1000

                        if  timeDelta >= timeout {
                            print("Timeout @ Delta \(timeDelta).")
                            break
                        }
                    }
                    
                    if let callback = self._requestCallbacks[correlationId] {
						self._requestCallbacks.removeValue(forKey: correlationId)
						callback(Data(bytes: bytes))
                    } else {
                        print(
                            "Unable to find reuqest callback for " +
                            "Correlation Id: \(correlationId)"
                        )
                    }
                } catch ConnectionError.zeroLengthResponse {
                    print("Zero Lenth Response")
                } catch ConnectionError.partialResponse(let size) {
                    print("Response Size: \(size) is invalid.")
                } catch ConnectionError.bytesNoLongerAvailable {
                    return
                } catch {
                    print("Error")
                }
            } else {
                print("Unable to find Input Stream")
            }

            //print("\tReleasing Input Stream Read")
        }
    }
	
	private static var correlationId: Int32 = 0
	func makeCorrelationId() -> Int32 {
		var id: Int32!
		DispatchQueue(label: "FranzMakeCorrelationId").sync {
			id = Connection.correlationId
			Connection.correlationId += 1
		}
		return id
	}
	
	func write<T: KafkaRequest>(_ request: T, callback: @escaping ((T.Response) -> Void)) {

		let corId = makeCorrelationId()
		_requestCallbacks[corId] = { data in
			var mutableData = data
			callback(T.Response.init(data: &mutableData))
		}
		let dispatchBlock = DispatchWorkItem(qos: .unspecified, flags: []) {
			if let stream = self.outputStream {
				if stream.hasSpaceAvailable {
					let data = request.data(correlationId: corId, clientId: self.clientId)
					
					data.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) -> Void in
						stream.write(bytes, maxLength: data.count)
					}
				} else {
					print("No Space Available for Writing")
				}
			}
		}
		
		if let outputStream = outputStream {
			if outputStream.hasSpaceAvailable {
				_outputStreamQueue.async(execute: dispatchBlock)
			} else {
				_writeRequestBlocks.append(dispatchBlock.perform)
			}
		} else {
			_writeRequestBlocks.append(dispatchBlock.perform)
		}
    }
	
	func writeBlocking<T: KafkaRequest>(_ request: T) -> T.Response {
		let semaphore = DispatchSemaphore(value: 0)
		var response: T.Response!
		write(request) { r in
			response = r
			semaphore.signal()
		}
		semaphore.wait()
		return response
	}
	
    private func getMessageMetadata() throws -> (Int32, Int32) {
        if let activeInputStream = inputStream {
            let length = responseLengthSize + responseCorrelationIdSize
            var buffer = Array<UInt8>(repeating: 0, count: Int(length))
            if activeInputStream.hasBytesAvailable {
				activeInputStream.read(&buffer, maxLength: Int(length))
            } else {
                throw ConnectionError.bytesNoLongerAvailable
            }
			let sizeBytes = buffer.prefix(upTo: Int(responseLengthSize))
			buffer.removeFirst(Int(responseLengthSize))
			
			var sizeData = Data(bytes: sizeBytes)
			let responseLengthSizeInclusive = Int32(data: &sizeData)
            
            if responseLengthSizeInclusive > 4 {
				let correlationIdSizeBytes = buffer.prefix(upTo: Int(responseCorrelationIdSize))
				buffer.removeFirst(Int(responseCorrelationIdSize))
				
				var correlationIdSizeData = Data(bytes: correlationIdSizeBytes)
                return (
                    responseLengthSizeInclusive - responseLengthSize,
                    Int32(data: &correlationIdSizeData)
                )
            } else if responseLengthSizeInclusive == 0 {
                throw ConnectionError.zeroLengthResponse
            } else {
                throw ConnectionError.partialResponse(size: responseLengthSizeInclusive)
            }
        }
        
        throw ConnectionError.unableToFindInputStream
    }
    
    func stream(_ aStream: Stream, handle eventCode: Stream.Event) {
        if let inputStream = aStream as? InputStream {
            let status = inputStream.streamStatus
            switch status {
            case .open:
                switch eventCode {
                case Stream.Event.hasBytesAvailable:
                    read()
                case Stream.Event.openCompleted:
                    return
                default:
                    print("STREAM EVENT: \(eventCode.description)")
                }
            case .reading:
                return
            case .error:
                print("INPUT STREAM ERROR: \(aStream.streamError?.localizedDescription ?? String())")
                return
            default:
                print("INPUT STREAM STATUS: \(aStream.streamStatus.description)")
                return
            }
        } else if let outputStream = aStream as? OutputStream {
            let status = outputStream.streamStatus
            switch status {
            case .open:
                switch eventCode {
                case Stream.Event.hasSpaceAvailable:
                    if _writeRequestBlocks.count > 0 {
                        let block = _writeRequestBlocks.removeFirst()
                        _outputStreamQueue.async(execute: block)
                    }
                case Stream.Event.openCompleted:
                    return
                default:
                    print("OUTPUT STREAM EVENT: \(eventCode.description)")
                }
            case .writing:
                return
            case .error:
                print("OUTPUT STREAM ERROR: \(aStream.streamError?.localizedDescription ?? String())")
                return
            default:
                print("OUTPUT STREAM STATUS:: \(aStream.streamStatus.description)")
                return
            }
        }
    }
}
