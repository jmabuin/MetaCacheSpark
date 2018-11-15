/**
 * Copyright 2017 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * This file is part of MetaCacheSpark.
 *
 * MetaCacheSpark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MetaCacheSpark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MetaCacheSpark. If not, see <http://www.gnu.org/licenses/>.
 */

package com.github.jmabuin.metacachespark.io;
import com.github.jmabuin.metacachespark.Sequence;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * This class define an InputFormat for FASTQ files for the 
 * Hadoop MapReduce framework.
 *
 * @author Mahmoud Parsian
 * @author José M. Abuín
 */
public class Fasta2SequenceInputFormat extends FileInputFormat<String,Sequence> {

    @Override
    public RecordReader<String, Sequence> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new Fasta2SequenceRecordReader();
    }
}