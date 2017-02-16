//
//  YapTaskQueueExtension.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation
import YapDatabase.YapDatabaseFilteredView
import YapDatabase

internal enum QueueState {
    case processing(YapTaskQueueAction)
    case paused(YapTaskQueueAction,Date)
}

public enum DatabaseStrings:String {
    case YapTasQueueMasterBrokerExtensionName = "YapTasQueueMasterBrokerExtensionName"
}

@objc public protocol YapTaskQueueHandler {
    
    /** This method is called when an item is available to be exectued. Call completion once finished with the action item.
     
     */
    func handleNextItem(_ action:YapTaskQueueAction, completion:(_ success:Bool, _ retryTimeout:TimeInterval)->Void)
}

/// YapTaskQueueBroker is a subclass of YapDatabaseFilteredView. It listens for changes and manages the timing of running an action.
open class YapTaskQueueBroker: YapDatabaseFilteredView {
    
    /// YapDatabaseConnection for listening to and getting database objects and changes.
    fileprivate var databaseConnection:YapDatabaseConnection? = nil
    
    /// This dictionary tracks what is currently going on so that only one item at a time is being executed.
    fileprivate var currentState = [String:QueueState]()
    
    /// This queue is used to guarentee that changes to the current work dictionary don't have any queue issues.
    fileprivate let isolationQueue = DispatchQueue(label: "YapTaskQueueExtension", attributes: [])
    
    /// This queue is just used for receiving in the NSNotifications
    fileprivate let notificationQueue = OperationQueue()
    
    /// This is where most operations in this class start on.
    fileprivate let workQueue = DispatchQueue(label: "YapTaskQueueBroker-GCDQUEUE", attributes: [])
    
    /// This is the Object that completes the operation and is handed a task to complete.
    open var handler:YapTaskQueueHandler
    
    /**
     Create a new YapTaskQueueBroker. After created the YapTaskQueueBroker needs to be registered with a database in order to get changes and objects.
     
     - parameters:
     - parentViewName: Should be the view name of the YapTaskQueueMasterBroker
     - handler: The handler that will be passed the items
     - filtering: This block should return true if this broker should handle a particular queue. Each queue is executed synchronously
     
     */
    internal init(parentViewName viewName: String, handler:YapTaskQueueHandler, filtering: @escaping (_ queueName:String) -> Bool) {
        self.notificationQueue.maxConcurrentOperationCount = 1
        self.handler = handler
        let filter = YapDatabaseViewFiltering.withKeyBlock { (transaction, group, collection, key) -> Bool in
            //Check if this group has already been 'claimed' by a queue broker
            return filtering(group)
        }
        super.init(parentViewName: viewName, filtering: filter, versionTag: nil, options:nil)
        
        // Best way for now to find when we are registred with a database
        self.addObserver(self, forKeyPath: "registeredDatabase", options: .new, context: nil)
    }
    
    /**
     Create a new YapTaskQueueBroker. This uses the view name as  prefix for all queues it handles.
     For example if the viewName is "MessageQueue" this will hanldle all queues that begin with "MessageQueue" like "MessageQueue-buddy1"
     
     - parameters:
     - parentViewName: Should be the view name of the YapTaskQueueMasterBroker
     - handler: The handler that will be passed the items
     */
    public convenience init(parentViewName viewName: String, name:String, handler:YapTaskQueueHandler) {
        
        self.init(parentViewName:viewName,handler:handler,filtering: { quename in
            return quename.hasPrefix(name)
        })
    }
    
    /// For now this seems to be the best way to find out with we are registred with the database
    override open func observeValue(forKeyPath keyPath: String?, of object: Any?, change: [NSKeyValueChangeKey : Any]?, context: UnsafeMutableRawPointer?) {
        self.didRegisterExtension()
    }
    
    /// Only way to get the current action for a given queue
    internal func actionForQueue(_ queue:String) -> YapTaskQueueAction? {
        
        
        //Ensure that some value was stored
        guard let currentState = self.stateForQueue(queue) else {
            return nil
        }
        
        // Only get action if the state was .Processing
        switch currentState {
        case .processing(let action):
            return action
        default:
            return nil
        }
    }
    
    /// Only way to get the current state for a given queue
    internal func stateForQueue(_ queue:String) -> QueueState? {
        var state:QueueState? = nil
        
        // Use isolation queue to access self.currentState
        self.isolationQueue.sync {
            state = self.currentState[queue]
        }
        
        return state
    }
    
    /// Only way to set or remove an action from the active action dictionary
    internal func setQueueState(_ state:QueueState?, queueName:String) {
        self.isolationQueue.async {
            if let act = state {
                self.currentState.updateValue(act, forKey: queueName)
            } else {
                self.currentState.removeValue(forKey: queueName)
            }
        }
    }
    
    /// Execute on registration with a database
    func didRegisterExtension() {
        
        NotificationCenter.default.addObserver(forName: NSNotification.Name.YapDatabaseModified, object: self.registeredDatabase, queue: self.notificationQueue) {[weak self] (notification) in
            if let strongSelf = self {
                strongSelf.processDatabaseNotification(notification)
            }
        }
        
        self.databaseConnection = self.registeredDatabase?.newConnection()
    }
    
    /// Takes in a datbase notification and checks if there are any updates for this view
    func processDatabaseNotification(_ notification:Notification) {
        
        guard let viewName = self.registeredName else {
            return
        }
        
        guard let connection = self.databaseConnection?.ext(viewName) as? YapDatabaseViewConnection else {
            return
        }
        
        if connection.hasChanges(for: [notification]) {
            
            
            
            self.checkForActions()
        }
    }
    
    /** Goes through all the queues and checks if there are more actions to execute */
    func checkForActions() {
        var groups:[String]? = nil
        self.databaseConnection?.asyncReadWrite({ (transaction) in
            guard let name = self.registeredName else {
                return
            }
            
            guard let viewTransaction = transaction.ext(name) as? YapDatabaseViewTransaction else {
                return
            }
            groups = viewTransaction.allGroups()
            
            }, completionQueue: self.workQueue, completionBlock: {
                if let groupsArray = groups {
                    
                    // We need to check all our processing and paused actions to see if their queues are still there.
                    // If the group is gone then we need to reset the state on that group
                    let keys = self.currentState.keys
                    for group in keys where !groupsArray.contains(group) {
                        self.setQueueState(nil, queueName: group)
                    }
                    
                    
                    for groupName in groupsArray {
                        self.checkQueue(groupName)
                    }
                }
                
        })
    }
    
    /** Go through a queue and check if there is an item to execute and no other action form that queue being executed. */
    func checkQueue(_ queueName:String) {
        var newAction:YapTaskQueueAction? = nil
        self.databaseConnection?.read({ (transaction) in
            guard let name = self.registeredName else {
                return
            }
            
            guard let viewTransaction = transaction.ext(name) as? YapDatabaseViewTransaction else {
                return
            }
            
            // Check if we have an object in the queue.
            // If we have no object then clear out the state
            // If we do have an object it needs to match the item being processed or paused. There can be a mismatch if the item it deleted from the database is deleted.
            // If it's deleted then we should force ourselves to move to the next item.
            if let action = viewTransaction.firstObject(inGroup: queueName) as? YapTaskQueueAction {
                switch self.stateForQueue(queueName) {
                case let .some(.paused(item,_)) where item.yapKey() != action.yapKey() || item.yapCollection() != action.yapCollection():
                    newAction = action
                case let .some(.processing(item)) where item.yapKey() != action.yapKey() || item.yapCollection() != action.yapCollection():
                    newAction = action
                case .none:
                    newAction = action
                default:
                    break
                }
            } else {
                self.setQueueState(nil, queueName: queueName)
            }
        })
        
        // If there is a new action then we send it to be handled and update the queue state otherwise we can stop
        // because the queue is doing something or paused
        guard let action = newAction else {
            return
        }
        
        //Set the state correctly as we're about to start processing a new action
        self.setQueueState(.processing(action), queueName: queueName)
        
        DispatchQueue.global().async(execute: {
            self.handler.handleNextItem(action, completion: { [weak self] (result, retryTimeout) in
                if let strongSelf = self {
                    if result {
                        strongSelf.databaseConnection?.readWrite({ (t) in
                            t.removeObject(forKey: action.yapKey(), inCollection: action.yapCollection())
                            strongSelf.setQueueState(nil, queueName: queueName)
                        })
                        
                        strongSelf.workQueue.async(execute: { [weak strongSelf] in
                            strongSelf?.checkQueue(queueName)
                            })
                        
                        
                    } else if retryTimeout == 0.0 {
                        strongSelf.checkQueue(queueName)
                    } else {
                        let date = Date(timeIntervalSinceNow: retryTimeout)
                        strongSelf.setQueueState(.paused(action,date), queueName: queueName)
                        if retryTimeout > 0 && retryTimeout < (Double(INT64_MAX) / Double(NSEC_PER_SEC)) {
                            let time = DispatchTime.now() + Double(Int64(retryTimeout * Double(NSEC_PER_SEC))) / Double(NSEC_PER_SEC)
                            strongSelf.workQueue.asyncAfter(deadline: time,execute: { [weak strongSelf] in
                                strongSelf?.restartQueueIfPaused(queueName)
                                })
                        }
                        
                    }
                    
                }
                })
            
        })
    }
    
    func restartQueueIfPaused(_ queueName:String) {
        //Remove paused state
        if let state = self.stateForQueue(queueName) {
            switch state {
            case .paused:
                self.setQueueState(nil, queueName: queueName)
            default:
                break
            }
        }
        self.checkQueue(queueName)
    }
    
    open func restartQueues(_ queueNames:[String]) {
        for name in queueNames {
            self.restartQueueIfPaused(name)
        }
    }
    
    deinit {
        self.removeObserver(self, forKeyPath: "registeredDatabase")
    }
    
}
extension YapTaskQueueBroker {
    /**
     Creates a string that is a queue name that is handled by this queue broker. If this queue broker name is "MessageQueue" then passing "buddy1" as the suffix
     yields "MessageQueue-buddy1"
     
     -parameter suffix: The suffix of the queue name
     -returns: A queue name that is unique to that suffix and handled by this queue broker
     */
    public func queueNameWithSuffix(_ suffix:String) throws ->  String {
        guard let name = self.registeredName else {
            throw YapTaskQueueError.noRegisteredViewName
        }
        return "\(name)-\(suffix)"
    }
}

//MARK: Class Methods
extension YapTaskQueueBroker {
    ///Use this method for convience. It automatically ensures a Master Broker is setup and registers the needed database views
    public class func setupWithDatabase(_ database:YapDatabase, name:String, handler:YapTaskQueueHandler) throws -> Self
    {
        return try setupWithDatabaseHelper(database, name: name, handler: handler)
    }
    
    public class func setupWithDatabaseHelper<T>(_ database:YapDatabase, name:String, handler:YapTaskQueueHandler) throws -> T
    {
        if database.registeredExtension(DatabaseStrings.YapTasQueueMasterBrokerExtensionName.rawValue) == nil {
            let masterBroker = YapTaskQueueMasterBroker(options: nil)
            let result = database.register(masterBroker, withName: DatabaseStrings.YapTasQueueMasterBrokerExtensionName.rawValue)
            if !result {
                throw YapTaskQueueError.cannotRegisterMasterView
            }
        }
        
        let queue = YapTaskQueueBroker(parentViewName: DatabaseStrings.YapTasQueueMasterBrokerExtensionName.rawValue, name:name, handler: handler)
        if !database.register(queue, withName: name) {
            throw YapTaskQueueError.cannotRegisterBrokerView
        }
        return queue as! T
    }
}
