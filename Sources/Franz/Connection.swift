//
//  Connection.swift
//  Franz
//
//  Created by Kellan Cummings on 1/13/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

typealias RequestCallback = ([UInt8]) -> ()

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

class KafkaConnection: NSObject, StreamDelegate {
    
    fileprivate var ipv4: String

    var apiVersion: ApiVersion {
        return ApiVersion.defaultVersion
    }

    fileprivate var _requestCallbacks = [Int32: RequestCallback]()
    fileprivate var _broker: Broker
    
    fileprivate var clientId: String
    
    fileprivate var readStream: Unmanaged<CFReadStream>?
    fileprivate var writeStream: Unmanaged<CFWriteStream>?

    fileprivate var inputStream: InputStream?
    fileprivate var outputStream: OutputStream?
    
    fileprivate var port: Int32
    
    fileprivate let responseLengthSize: Int32 = 4
    fileprivate let responseCorrelationIdSize: Int32 = 4

    fileprivate var _inputStreamQueue: DispatchQueue
    fileprivate var _outputStreamQueue: DispatchQueue
    fileprivate var _writeRequestBlocks = [()->()]()
    
    init(ipv4: String, port: Int32, broker: Broker, clientId: String) {
        self.ipv4 = ipv4
        self.clientId = clientId
        self._broker = broker
        self.port = port

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

        self.inputStream?.delegate = self
        self.inputStream?.schedule(
            in: RunLoop.main,
            forMode: RunLoopMode.defaultRunLoopMode
        )

        self.outputStream?.delegate = self
        self.outputStream?.schedule(
            in: RunLoop.main,
            forMode: RunLoopMode.defaultRunLoopMode
        )

        self.inputStream?.open()
        self.outputStream?.open()
    }
    
    fileprivate func read(_ timeout: Double = 3000) {
        //print("Read Block Added")
        _inputStreamQueue.async {
            //print("\tBeginning Input Stream Read")
            if let inputStream = self.inputStream {
                do {
                    let (size, correlationId) = try self.getMessageMetadata()
                    //print("\t\tReading Bytes")
                    var bytes = [UInt8]()
                    let startTime = Date().timeIntervalSince1970
                    while bytes.count < Int(size) {
                        
                        if inputStream.hasBytesAvailable {
                            var buffer = [UInt8](repeating: 0, count: Int(size))
                            let bytesInBuffer = inputStream.read(&buffer, maxLength: Int(size))
                            //print("\t\tReading Bytes")
                            buffer = buffer.slice(0, length: bytesInBuffer)
                            //print("BUFFER(\(size)): \(buffer)")
                            bytes += buffer
                        }
                        
                        let currentTime = Date().timeIntervalSince1970
                        let timeDelta = (currentTime - startTime) * 1000

                        if  timeDelta >= timeout {
                            print("Timeout @ Delta \(timeDelta).")
                            break
                        }
                    }
                    
                    if let callback = self._requestCallbacks[correlationId] {
                        callback(bytes)
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

    func write(_ request: KafkaRequest, callback: RequestCallback? = nil) {
        request.clientId = KafkaString(value: clientId)
        //print("Write Block Added")
        if let requestCallback = callback {
            _requestCallbacks[request.correlationId] = requestCallback
        }
		let dispatchBlock = DispatchWorkItem(qos: .unspecified, flags: []) {
			//print("\tBeginning Output Stream Write")
			if let stream = self.outputStream {
				if stream.hasSpaceAvailable {
					let data = request.data
					//print("Data: \(data)")
					let bytesPtr = (data as NSData).bytes
					let bytes = bytesPtr.assumingMemoryBound(to: UInt8.self)
					//print("Writing to Output Stream: \(data.length)")
					stream.write(bytes, maxLength: data.count)
					//print("\tReleasing Output Stream Write(\(data.length))")
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
    
    fileprivate func getMessageMetadata() throws -> (Int32, Int32) {
        if let activeInputStream = inputStream {
            let length = responseLengthSize + responseCorrelationIdSize
            var buffer = Array<UInt8>(repeating: 0, count: Int(length))
            if activeInputStream.hasBytesAvailable {
				activeInputStream.read(&buffer, maxLength: Int(length))
            } else {
                throw ConnectionError.bytesNoLongerAvailable
            }
            
            let sizeBytes = buffer.slice(0, length: Int(responseLengthSize))
            let responseLengthSizeInclusive = Int32(bytes: sizeBytes)
            
            if responseLengthSizeInclusive > 4 {
                return (
                    responseLengthSizeInclusive - responseLengthSize,
                    Int32(bytes: buffer.slice(0, length: Int(responseCorrelationIdSize)))
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
        //print("STREAM STATUS: \(aStream.streamStatus.description) => \(eventCode.description)")
        if let inputStream = aStream as? InputStream {
            let status = inputStream.streamStatus
            //print("INPUT STREAM STATUS: \(status.description) => \(eventCode.description)")
            switch status {
            case .open:
                switch eventCode {
                case Stream.Event.hasBytesAvailable:
                    //print("Input Stream Has Bytes Available")
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
            //print("OUTPUT STREAM STATUS: \(status.description) => \(eventCode.description)")
            switch status {
            case .open:
                switch eventCode {
                case Stream.Event.hasSpaceAvailable:
                    //print("Requests: \(_writeRequestBlocks.count)")
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
    
    deinit {
        print("Deinitializing.")
    }
}
