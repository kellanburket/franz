//
//  DockerTestBase.swift
//  FranzTests
//
//  Created by Luke Lau on 21/08/2017.
//

import XCTest
@testable import Franz

class DockerTestBase: XCTestCase {
    
	static var docker: Process!
	static let startedSemaphore = DispatchSemaphore(value: 0)
	
	static let compose = URL(fileURLWithPath: "/usr/local/bin/docker-compose")
	static let yml = Bundle(for: DockerTestBase.self).url(forResource: "docker-compose", withExtension: "yml")!
	
	internal static let host = "localhost"
	internal static let port: Int32 = 9092
	
	override class func setUp() {
		do {
			
			if #available(OSX 10.13, *) {
				try Process.run(compose, arguments: ["-f", yml.path, "down"]).waitUntilExit()
				try docker = Process.run(compose, arguments: ["-f", yml.path, "up", "-d"])
			} else {
				Process.launchedProcess(launchPath: compose.path, arguments: ["-f", yml.path, "down"]).waitUntilExit()
				docker = Process.launchedProcess(launchPath: compose.path, arguments: ["-f", yml.path, "up", "-d"])
			}
			
			waitForKafka()
			
		} catch {
			fatalError("Couldn't find docker-compose")
		}
	}
	
	class func waitForKafka() {
		let delegate = DockerStreamDelegate()
		var inputStream: InputStream?, outputStream: OutputStream?
		var timer: Timer?
		
		var runLoopToStop: CFRunLoop!
		
		DispatchQueue(label: "FranzDockerStreamPoll").async {
			if #available(OSX 10.12, *) {
				timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { _ in
					
					if !delegate.retry {
						return
					}
					
					delegate.written = false
					delegate.retry = false
					
					inputStream?.close()
					outputStream?.close()
					
					Stream.getStreamsToHost(withName: DockerTestBase.host, port: Int(port), inputStream: &inputStream, outputStream: &outputStream)
					
					inputStream!.delegate = delegate
					outputStream!.delegate = delegate
					
					delegate.inputStream = inputStream
					delegate.outputStream = outputStream
					
					inputStream!.schedule(in: .current, forMode: .commonModes)
					outputStream!.schedule(in: .current, forMode: .commonModes)
					
					inputStream!.open()
					outputStream!.open()
				}
			} else {
				fatalError("Upgrade to macOS 10.12 to run Docker tests")
			}
			runLoopToStop = CFRunLoopGetCurrent()
			CFRunLoopRun()
		}
		startedSemaphore.wait()
		
		CFRunLoopStop(runLoopToStop)
		
		print("Kafka server is ready")
		
		inputStream?.close()
		outputStream?.close()
		
		timer?.invalidate()
	}
	
	class DockerStreamDelegate: NSObject, StreamDelegate {
		
		var inputStream: InputStream!, outputStream: OutputStream!
		var retry = true
		var written = false
		
		func stream(_ aStream: Stream, handle eventCode: Stream.Event) {
			if eventCode == .errorOccurred {
				retry = true
			}
			if aStream == inputStream, eventCode == .hasBytesAvailable {
				
				//Attempt to read in the size of the list groups request
				var data = Data(capacity: 8)
				data.withUnsafeMutableBytes { (bytes: UnsafeMutablePointer<UInt8>) -> Void in
					
					inputStream.read(bytes, maxLength: 8)
					var readData = Data(buffer: UnsafeBufferPointer(start: bytes, count: 18))
					let responseSize = UInt32(data: &readData)
					let correlationId = UInt32(data: &readData)
					
					//If it's a zero-length response, retry
					if responseSize > 0, correlationId == 1234 {
						startedSemaphore.signal()
					} else {
						retry = true
					}
				}
			}
			if aStream == outputStream, eventCode == .hasSpaceAvailable, !written {
				print("Trying to contact Kafka server")
				let req = ListGroupsRequest()
				let data = req.data(correlationId: 1234, clientId: "test")
					
				data.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) -> Void in
					outputStream.write(bytes, maxLength: data.count)
				}
				
				written = true
			}
		}
		
	}
	
	override class func tearDown() {
		do {
			if #available(OSX 10.13, *) {
				try Process.run(compose, arguments: ["-f", yml.path, "stop"]).waitUntilExit()
			} else {
				Process.launchedProcess(launchPath: compose.path, arguments: ["-f", yml.path, "stop"]).waitUntilExit()
			}
		} catch {
			print("Failed to stop containers")
		}
	}
    
}
