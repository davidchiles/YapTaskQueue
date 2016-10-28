//
//  YapTaskQueueTests.swift
//  YapTaskQueueTests
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import XCTest
import YapDatabase
@testable import YapTaskQueue

class TestActionObject:NSObject, YapTaskQueueAction, NSCoding, NSCopying {
    var key:String
    var collection:String
    
    var name:String
    var queue:String
    var date = NSDate()
    
    init(key:String, collection:String, name:String, queue:String) {
        self.key = key
        self.collection = collection
        self.name = name
        self.queue = queue
    }
    
    func yapKey() -> String {
        return self.key
    }
    
    func yapCollection() -> String {
        return self.collection
    }
    
    func sort(otherObject: YapTaskQueueAction) -> NSComparisonResult {
        guard let otherAction = otherObject as? TestActionObject else {
            return .OrderedSame
        }
        return self.date.compare(otherAction.date)
    }
    
    func queueName() -> String {
        return self.queue
    }
    
    ///NSCopying
    func copyWithZone(zone: NSZone) -> AnyObject {
        let copy = TestActionObject(key: self.key, collection: self.collection, name: self.name, queue: self.queue)
        copy.date = self.date
        return copy
    }
    
    ///NSCoding
    internal func encodeWithCoder(aCoder: NSCoder) {
        aCoder.encodeObject(self.name, forKey: "name")
        aCoder.encodeObject(self.queue, forKey: "queue")
        aCoder.encodeObject(self.date, forKey: "date")
        aCoder.encodeObject(self.key, forKey: "key")
        aCoder.encodeObject(self.collection, forKey: "collection")
    }
    
    internal required convenience init?(coder aDecoder: NSCoder) {
        let name = aDecoder.decodeObjectForKey("name") as! String
        let queue = aDecoder.decodeObjectForKey("queue") as! String
        let date = aDecoder.decodeObjectForKey("date") as! NSDate
        let key = aDecoder.decodeObjectForKey("key") as! String
        let collection = aDecoder.decodeObjectForKey("collection") as! String
        
        self.init(key:key, collection: collection, name: name, queue: queue)
        self.date = date
    }
}

public class TestHandler:YapTaskQueueHandler {
    
    var handleBlock:(action:TestActionObject) -> (Bool,NSTimeInterval)
    var connection:YapDatabaseConnection?
    
    init(handleBlock:(TestActionObject) -> (Bool,NSTimeInterval)) {
        self.handleBlock = handleBlock
    }
    
    @objc public func handleNextItem(action: YapTaskQueueAction, completion: (success: Bool, retryTimeout: NSTimeInterval) -> Void) {
    
        guard let testObject = action as? TestActionObject  else {
            completion(success: false, retryTimeout: DBL_MAX)
            return
        }
        
        let (result,retryTimout) =  self.handleBlock(action: testObject)
        completion(success: result, retryTimeout: retryTimout)
    }
}

func deleteFiles(url:NSURL) {
    let fileManager = NSFileManager.defaultManager()
    let enumerator = fileManager.enumeratorAtURL(url, includingPropertiesForKeys: nil, options: NSDirectoryEnumerationOptions(), errorHandler: nil)
    while let file = enumerator?.nextObject() as? NSURL {
        try! fileManager.removeItemAtURL(file)
    }
}

func createDatabase(suffix:String) -> YapDatabase {
    let paths = NSSearchPathForDirectoriesInDomains(.CachesDirectory, .UserDomainMask, true)
    
    let baseDir = NSURL.fileURLWithPath(paths.first ?? NSTemporaryDirectory())
    deleteFiles(baseDir)
    let file = NSURL.fileURLWithPath(#file).lastPathComponent!.componentsSeparatedByString(".").first!
    let name = "\(file)-\(suffix).sqlite"
    let path = baseDir.URLByAppendingPathComponent(name)!.path
    // Setup datbase
    return YapDatabase(path: path!)
}

func setupDatabase(suffix:String) -> YapDatabase {
    let database = createDatabase(suffix)
    
    // Setup Extension
    let options = YapDatabaseViewOptions()
    let master = YapTaskQueueMasterBroker(options: options)
    database.registerExtension(master, withName: "master")
    return database
}

class YapTaskQueueTests: XCTestCase {
    
    
    override func setUp() {
        super.setUp()
    }
    
    override func tearDown() {

        super.tearDown()
    }
    
    func setupQueue(database:YapDatabase, handler:TestHandler, actionCount:Int, name:String) {
        let connection = database.newConnection()
        let ext = YapTaskQueueBroker(parentViewName: "master", handler: handler, filtering: { (threadName) -> Bool in
            return threadName == name
        })
        database.registerExtension(ext, withName: "broker-\(name)")
        
        connection.asyncReadWriteWithBlock({ (transaction) in
            for actionIndex in 0..<actionCount {
                let actionName = "\(actionIndex)"
                let action = TestActionObject(key: actionName, collection: "collection\(name)", name: actionName, queue: name)
                
                transaction.setObject(action, forKey: action.key, inCollection: action.collection)
                
            }
        })
    }
    
    func testMoveFromOneQueueToAnother() {
        let database = createDatabase(#function)
        let connection = database.newConnection()
        let expectation = self.expectationWithDescription(#function)
        
        let firstHanlder = TestHandler { (action) -> (Bool,NSTimeInterval) in
            let newAction = action.copy() as! TestActionObject
            newAction.key = "newKey"
            newAction.queue = "handler2-queue"
            connection.readWriteWithBlock({ (transaction) in
                transaction.setObject(newAction, forKey: newAction.yapKey(), inCollection: newAction.yapCollection())
            })
            return (true,0)
        }
        let secondHandler = TestHandler { (action) -> (Bool,NSTimeInterval) in
            expectation.fulfill()
            return (true,0)
        }
        let handler1 = try! YapTaskQueueBroker.setupWithDatabase(database, name: "handler1", handler: firstHanlder)
        _ = try! YapTaskQueueBroker.setupWithDatabase(database, name: "handler2", handler: secondHandler)
        let queueName = try! handler1.queueNameWithSuffix("queue")
        let action = TestActionObject(key: "key", collection: "collection", name: "name", queue:queueName)
        connection.readWriteWithBlock { (transaction) in
            transaction.setObject(action, forKey: action.yapKey(), inCollection: action.yapCollection())
        }
        
        
        self.waitForExpectationsWithTimeout(10, handler: nil)
        
    }
    
    func testOneAction() {
        let database  = setupDatabase(#function)
        let expectation = self.expectationWithDescription("test one action")
        
        let handler = TestHandler { (action) -> (Bool,NSTimeInterval) in
            print("\(action.name) - \(action.date)")
            expectation.fulfill()
            return (true,0)
        }
        handler.connection = database.newConnection()
        
        let ext = YapTaskQueueBroker(parentViewName: "master", handler: handler, filtering: { (threadName) -> Bool in
            return true
            })
        database.registerExtension(ext, withName: "broker")
        
        database.newConnection().readWriteWithBlock({ (transaction) in
            let action = TestActionObject(key: "key", collection: "collection", name: "name", queue: "default")
            transaction .setObject(action, forKey: action.key, inCollection: action.collection)
        })
        
        self.waitForExpectationsWithTimeout(10, handler: nil)
    }
    
    func testMultipleActionsOneThread() {
        let database  = setupDatabase(#function)
        var currentCount = 0
        let count = 10
        let expectation = self.expectationWithDescription("testMultipleActionsOneThread")
        let handler = TestHandler { (action) -> (Bool,NSTimeInterval) in
            
            let nameInt = Int(action.name)
            print("\(currentCount) \(nameInt)")
            XCTAssert(currentCount == nameInt,"Expect Item: \(currentCount) - Recieved: \(nameInt!)")
            
            
            if (count-1 == currentCount) {
                expectation.fulfill()
            }
            currentCount += 1
            return (true,0)
        }
        handler.connection = database.newConnection()
        
        let connection = database.newConnection()
        
        
        let ext = YapTaskQueueBroker(parentViewName: "master", handler: handler, filtering: { (queueName) -> Bool in
            return true
        })
        database.registerExtension(ext, withName: "broker")
        
        for index in 0..<count {
            let name = "\(index)"
            let action = TestActionObject(key: name, collection: "collection", name: name, queue: "default")
            connection.asyncReadWriteWithBlock({ (transaction) in
                transaction.setObject(action, forKey: action.key, inCollection: action.collection)
            })
        }
        
        self.waitForExpectationsWithTimeout(100, handler: nil)
    }
    
    func testSteup() {
        let database = createDatabase(#function)
        let handler = TestHandler { (action) -> (Bool,NSTimeInterval) in
            return (true,0)
        }
        let broker = try! YapTaskQueueBroker.setupWithDatabase(database, name: "queue1", handler: handler)
        XCTAssertNotNil(broker,"Error Setting up database")
        let ext = database.registeredExtension("queue1")
        XCTAssertNotNil(ext,"No extension registered")
    }
    
    func testMultipleActionsMultipleThreads () {
        let database = setupDatabase(#function)
        let threadCount = 5
        for threadIndex in 0..<threadCount {
            let expectation = self.expectationWithDescription("test Multiple \(threadIndex)")
            let actionCount = (threadIndex+1) * 5
            var currentCount = 0
            let handler = TestHandler(handleBlock: { (action) -> (Bool,NSTimeInterval) in
                let actionNumber = Int(action.name)!
                print("\(threadIndex): \(currentCount) - \(actionNumber)")
                XCTAssertEqual(currentCount, actionNumber,"\(threadIndex): \(currentCount) - \(actionNumber)")
                
                if (actionCount-1 == currentCount) {
                    expectation.fulfill()
                }
                
                currentCount+=1
                
                return (true,0)
            })
            
            self.setupQueue(database, handler: handler, actionCount: actionCount, name: "\(threadIndex)")
        }
        
        self.waitForExpectationsWithTimeout(1000, handler: nil)
        
    }
    
    func testPausingAction() {
        let expectation = self.expectationWithDescription(#function)
        let database = setupDatabase(#function)
        
        var count = 0
        
        // This handler waits to be called a second time in order to fulfill the expectation. 
        // The first time through it returns that the action failed and it should wait an indefinite amount of time before restarting the task.
        let startDate = NSDate()
        let delay = 2.0
        let handler = TestHandler { (action) -> (Bool, NSTimeInterval) in
            print("handled \(count)")
            
            count += 1
            if (count == 2) {
                let timeDifference = abs(startDate.timeIntervalSinceNow)
                XCTAssertEqualWithAccuracy(timeDifference, delay, accuracy: 0.5)
                expectation.fulfill()
            }
            return (false,DBL_MAX)
        }
        //Setup the queue with one action
        self.setupQueue(database, handler: handler, actionCount: 1, name: "queue")
        
        // After 2 seconds (should be enough time for the action to fail the first time) we tryto restart the queue if it has a paused action.
        let time = dispatch_time(DISPATCH_TIME_NOW, Int64(delay * Double(NSEC_PER_SEC)))
        let queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0)
        dispatch_after(time, queue) { 
            if let queue = database.registeredExtension("broker-queue") as? YapTaskQueueBroker {
                queue.restartQueueIfPaused("queue")
            }
        }
        
        self.waitForExpectationsWithTimeout(100, handler: nil)
    }
    
    func testPausingActionWithTimeout() {
        let exectation = self.expectationWithDescription(#function)
        let database = setupDatabase(#function)
        let delay = 3.0
        let startDate = NSDate()
        var count = 0
        let handler = TestHandler { (action) -> (Bool, NSTimeInterval) in
            count += 1
            
            //After the first one fails once it then succeeds
            if count == 2 {
                let timeDifference = abs(startDate.timeIntervalSinceNow)
                
                XCTAssertEqualWithAccuracy(timeDifference, delay, accuracy: 0.5)
                return (true,0)
            }
            // This is the third time through so we're done with the test
            else if (count == 3) {
                
                exectation.fulfill()
            }
            
            return(false,delay)
        }
        
        self.setupQueue(database, handler: handler, actionCount: 2, name: "queue")
        
        
        self.waitForExpectationsWithTimeout(100, handler: nil)
    }
    
}
