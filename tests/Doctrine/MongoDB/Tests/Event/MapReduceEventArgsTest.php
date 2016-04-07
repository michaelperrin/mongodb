<?php

namespace Doctrine\MongoDB\Tests\Event;

use Doctrine\MongoDB\Event\MapReduceEventArgs;

class MapReduceEventArgsTest extends \PHPUnit_Framework_TestCase
{
    public function testMapReduceEventArgs()
    {
        $invoker = new \stdClass();
        $map = new \MongoDB\BSON\JavaScript('');
        $reduce = new \MongoDB\BSON\JavaScript('');
        $out = array('inline' => true);
        $query = array('x' => 1);
        $options = array('finalize' => new \MongoDB\BSON\JavaScript(''));

        $mapReduceEventArgs = new MapReduceEventArgs($invoker, $map, $reduce, $out, $query, $options);

        $this->assertSame($invoker, $mapReduceEventArgs->getInvoker());
        $this->assertSame($map, $mapReduceEventArgs->getMap());
        $this->assertSame($reduce, $mapReduceEventArgs->getReduce());
        $this->assertSame($out, $mapReduceEventArgs->getOut());
        $this->assertSame($query, $mapReduceEventArgs->getQuery());
        $this->assertSame($options, $mapReduceEventArgs->getOptions());

        $map2 = new \MongoDB\BSON\JavaScript('a');
        $reduce2 = new \MongoDB\BSON\JavaScript('b');
        $out2 = array('inline' => false);
        $query2 = array('x' => 2);
        $options2 = array('finalize' => new \MongoDB\BSON\JavaScript('c'));

        $mapReduceEventArgs->setMap($map2);
        $mapReduceEventArgs->setReduce($reduce2);
        $mapReduceEventArgs->setOut($out2);
        $mapReduceEventArgs->setQuery($query2);
        $mapReduceEventArgs->setOptions($options2);

        $this->assertSame($map2, $mapReduceEventArgs->getMap());
        $this->assertSame($reduce2, $mapReduceEventArgs->getReduce());
        $this->assertSame($out2, $mapReduceEventArgs->getOut());
        $this->assertSame($query2, $mapReduceEventArgs->getQuery());
        $this->assertSame($options2, $mapReduceEventArgs->getOptions());
    }
}
