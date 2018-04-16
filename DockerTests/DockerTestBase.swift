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
	
	override class func setUp() {
		do {
			
			if #available(OSX 10.13, *) {
				try Process.run(compose, arguments: ["-f", yml.path, "stop"]).waitUntilExit()
				try Process.run(compose, arguments: ["-f", yml.path, "rm", "-f"]).waitUntilExit()
				try docker = Process.run(compose, arguments: ["-f", yml.path, "up", "-d"])
			} else {
				Process.launchedProcess(launchPath: compose.path, arguments: ["-f", yml.path, "stop"]).waitUntilExit()
				Process.launchedProcess(launchPath: compose.path, arguments: ["-f", yml.path, "rm", "-f"]).waitUntilExit()
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
		
		DispatchQueue(label: "dockerStreamPoll").async {
			if #available(OSX 10.12, *) {
				timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { _ in
					
					if !delegate.retry {
						return
					}
					
					delegate.written = false
					delegate.retry = false
					
					inputStream?.close()
					outputStream?.close()
					
					Stream.getStreamsToHost(withName: "localhost", port: 9092, inputStream: &inputStream, outputStream: &outputStream)
					
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
				var data = Data(capacity: 4)
				data.withUnsafeMutableBytes { (bytes: UnsafeMutablePointer<UInt8>) -> Void in
					
					inputStream.read(bytes, maxLength: 4)
					var readData = Data(buffer: UnsafeBufferPointer(start: bytes, count: 4))
					let responseSize = UInt32(data: &readData)
					
					//If it's a zero-length response, retry
					if responseSize > 0 {
						startedSemaphore.signal()
					} else {
						retry = true
					}
				}
			}
			if aStream == outputStream, eventCode == .hasSpaceAvailable, !written {
				print("Trying to contact Kafka server")
				let req = ListGroupsRequest()
				let data = req.data(correlationId: 0, clientId: nil)
					
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
