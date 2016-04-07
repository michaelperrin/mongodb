<?php

namespace Doctrine\MongoDB\Tests;

use Doctrine\MongoDB\LoggableCollection;

class LoggableCollectionTest extends \PHPUnit_Framework_TestCase
{
    const collectionName = 'collectionName';
    const databaseName = 'databaseName';

    public function testLog()
    {
        $called = false;

        $loggerCallable = function($msg) use (&$called) {
            $called = $msg;
        };

        $collection = $this->getTestLoggableCollection($loggerCallable);
        $collection->log(array('test' => 'test'));

        $this->assertEquals(array('collection' => self::collectionName, 'db' => self::databaseName, 'test' => 'test'), $called);
    }

    private function getTestLoggableCollection($loggerCallable)
    {
        $database = $this->getMockBuilder('Doctrine\MongoDB\Database')
            ->disableOriginalConstructor()
            ->getMock();

        $database->expects($this->any())
            ->method('getCollectionName')
            ->will($this->returnValue(self::databaseName));

        $mongoCollection = $this->getMockBuilder('MongoDB\Collection')
            ->disableOriginalConstructor()
            ->getMock();

        $mongoCollection->expects($this->any())
            ->method('getCollectionName')
            ->will($this->returnValue(self::collectionName));

        $eventManager = $this->getMockBuilder('Doctrine\Common\EventManager')
            ->disableOriginalConstructor()
            ->getMock();

        return new LoggableCollection($database, $mongoCollection, $eventManager, 0, $loggerCallable);
    }
}
