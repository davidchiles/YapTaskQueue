//
//  YapTaskQueueExtension.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation
import YapDatabase.YapDatabaseFilteredView

public protocol YapTaskQueueThreadHandler {
    //TODO: Change to completion block?
    func handleNextItem(action:YapTaskQueueAction) -> Bool
}

public class YapTaskQueueBroker: YapDatabaseFilteredView {
    
    private var databaseConnection:YapDatabaseConnection? = nil
    private let isolationQueue = dispatch_queue_create("YapTaskQueueExtension", DISPATCH_QUEUE_SERIAL)
    private let operationQueue = NSOperationQueue()
    private var handler:YapTaskQueueThreadHandler
    private var currentWork = [String:YapTaskQueueAction]()
    
    public init(parentViewName viewName: String, handler:YapTaskQueueThreadHandler, filtering: (threadName:String) -> Bool) {
        self.operationQueue.maxConcurrentOperationCount = 1
        self.handler = handler
        let filter = YapDatabaseViewFiltering.withKeyBlock { (transaction, group, collection, key) -> Bool in
            return filtering(threadName: group)
        }
        super.init(parentViewName: viewName, filtering: filter, versionTag: nil, options:nil)
        self.addObserver(self, forKeyPath: "registeredDatabase", options: .New, context: nil)
    }
    
    override public func observeValueForKeyPath(keyPath: String?, ofObject object: AnyObject?, change: [String : AnyObject]?, context: UnsafeMutablePointer<Void>) {
        self.didRegisterExtension()
    }
    
    internal func actionForThread(thread:String) -> YapTaskQueueAction? {
        var action:YapTaskQueueAction? = nil
        dispatch_sync(self.isolationQueue) { 
            action = self.currentWork[thread]
        }
        return action
    }
    
    internal func setAction(action:YapTaskQueueAction?, thread:String) {
        dispatch_async(self.isolationQueue) {
            if let act = action {
                self.currentWork.updateValue(act, forKey: thread)
            } else {
                self.currentWork.removeValueForKey(thread)
            }
        }
    }
    
    func didRegisterExtension() {
        
        NSNotificationCenter.defaultCenter().addObserverForName(YapDatabaseModifiedNotification, object: self.registeredDatabase, queue: self.operationQueue) {[weak self] (notification) in
            if let strongSelf = self {
                strongSelf.processDatabaseNotification(notification)
            }
        }
        
        self.databaseConnection = self.registeredDatabase?.newConnection()
    }
    
    func processDatabaseNotification(notification:NSNotification) {
        
        guard let viewName = self.registeredName else {
            return
        }
        
        guard let connection = self.databaseConnection?.ext(viewName) as? YapDatabaseViewConnection else {
            return
        }
        
        if connection.hasChangesForNotifications([notification]) {
            self.checkForActions()
        }
    }
    
    func checkForActions() {
        var groups:[String]? = nil
        self.databaseConnection?.asyncReadWriteWithBlock({ (transaction) in
            guard let name = self.registeredName else {
                return
            }
            
            guard let viewTransaction = transaction.ext(name) as? YapDatabaseViewTransaction else {
                return
            }
            groups = viewTransaction.allGroups()
            }, completionQueue: self.operationQueue.underlyingQueue, completionBlock: {
                if let groupsArray = groups {
                    for groupName in groupsArray {
                        self.checkThread(groupName)
                    }
                }
                
        })
        self.databaseConnection?.asyncReadWriteWithBlock({ (transaction) in
            
            
            
        })
    }
    
    func checkThread(threadName:String) {
        var newAction:YapTaskQueueAction? = nil
        self.databaseConnection?.asyncReadWriteWithBlock({ (transaction) in
            guard let name = self.registeredName else {
                return
            }
            
            guard let viewTransaction = transaction.ext(name) as? YapDatabaseViewTransaction else {
                return
            }
            
            viewTransaction.enumerateKeysAndObjectsInGroup(threadName, usingBlock: { (collection, key, object, row, stop) in
                
                guard let act = object as? YapTaskQueueAction else {
                    return
                }
                
                guard let queueName = act.queueName() else {
                    return
                }
                
                //If currently working don't do anymore work on that queue
                if self.actionForThread(queueName) == nil {
                    self.setAction(act, thread: threadName)
                    newAction = act
                }
                
                stop.initialize(true)
            })
            
            }, completionQueue: self.operationQueue.underlyingQueue, completionBlock: {
                guard let action = newAction else {
                    return
                }
                
                dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), {
                    let result = self.handler.handleNextItem(action)
                    if result {
                        self.databaseConnection?.readWriteWithBlock({ (t) in
                            t.removeObjectForKey(action.yapKey(), inCollection: action.yapCollection())
                            self.setAction(nil, thread: threadName)
                            
                        })
                        
                        self.operationQueue.addOperationWithBlock({ 
                            self.checkThread(threadName)
                        })
                        
                        
                    } else {
                        
                    }
                })
        })
    }
    
    deinit {
        self.removeObserver(self, forKeyPath: "registeredDatabase")
    }
}