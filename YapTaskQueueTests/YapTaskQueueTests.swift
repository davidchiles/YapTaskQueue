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

class TestActionObject:NSObject, YapTaskQueueAction, NSCoding {
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
    
    func queueName() -> String? {
        return self.queue
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

public class TestHandler:YapTaskQueueThreadHandler {
    
    var handleBlock:(action:TestActionObject) -> Bool
    var connection:YapDatabaseConnection?
    
    init(handleBlock:(TestActionObject) -> Bool) {
        self.handleBlock = handleBlock
    }
    
    public func handleNextItem(action: YapTaskQueueAction) -> Bool {
        guard let testObject = action as? TestActionObject  else {
            return true
        }
        
        let result =  self.handleBlock(action: testObject)
        self.connection?.readWriteWithBlock({ (transaction) in
            transaction.removeObjectForKey(testObject.key, inCollection: testObject.collection)
        })
        return result
    }
}

func deleteFiles(url:NSURL) {
    let fileManager = NSFileManager.defaultManager()
    let enumerator = fileManager.enumeratorAtURL(url, includingPropertiesForKeys: nil, options: NSDirectoryEnumerationOptions(), errorHandler: nil)
    while let file = enumerator?.nextObject() as? String {
        try! fileManager.removeItemAtURL(url.URLByAppendingPathComponent(file))
    }
}

func setupDatabase(suffix:String) -> YapDatabase {
    let paths = NSSearchPathForDirectoriesInDomains(.CachesDirectory, .UserDomainMask, true)

   let baseDir = NSURL.fileURLWithPath(paths.first ?? NSTemporaryDirectory())
    deleteFiles(baseDir)
    let file = NSURL.fileURLWithPath(#file).lastPathComponent!.componentsSeparatedByString(".").first!
    let name = "\(file)-\(suffix).sqlite"
    let path = baseDir.URLByAppendingPathComponent(name).path
    // Setup datbase
    let database = YapDatabase(path: path!)
    
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
    
    func testOneAction() {
        let database  = setupDatabase(#function)
        let expectation = self.expectationWithDescription("test one action")
        
        let handler = TestHandler { (action) -> Bool in
            print("\(action.name) - \(action.date)")
            expectation.fulfill()
            return true
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
        let handler = TestHandler { (action) -> Bool in
            
            let nameInt = Int(action.name)
            print("\(currentCount) \(nameInt)")
            XCTAssert(currentCount == nameInt,"\(currentCount) \(nameInt)")
            
            
            if (count-1 == currentCount) {
                expectation.fulfill()
            }
            currentCount += 1
            return true
        }
        handler.connection = database.newConnection()
        
        let connection = database.newConnection()
        
        
        let ext = YapTaskQueueBroker(parentViewName: "master", handler: handler, filtering: { (threadName) -> Bool in
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
    
    func testMultipleActionsMultipleThreads () {
        let databae = setupDatabase(#function)
        
        
    }
    
}
