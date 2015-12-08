/*
 * Copyright (C) 2007 by
 * 
 * 	Xuan-Hieu Phan
 *	hieuxuan@ecei.tohoku.ac.jp or pxhieu@gmail.com
 * 	Graduate School of Information Sciences
 * 	Tohoku University
 * 
 *  Cam-Tu Nguyen
 *  ncamtu@gmail.com
 *  College of Technology
 *  Vietnam National University, Hanoi
 *
 * JGibbsLDA is a free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License,
 * or (at your option) any later version.
 *
 * JGibbsLDA is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JGibbsLDA; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 */

package lda.jgibblda;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.FileNotFoundException;

public class LDA
{
    public static void main(String args[])
    {
        LDACmdOption option = new LDACmdOption();
        CmdLineParser parser = new CmdLineParser(option);

        args = new String[]{"-est", "-model", "JLDAModel", "-alpha", "0.5",
                "-beta", "0.1", "-ntopics", "20", "-niters", "25", "-twords",
                "20", "-dir", "G:/model", "-dfile", "all_classify.gz"};

        try {
            if (args.length == 0){
                showHelp(parser);
                return;
            }

            parser.parseArgument(args);

            if (option.est || option.estc){
                Long startTime = System.nanoTime();

                Estimator estimator = new Estimator(option);
                estimator.estimate();

                Double elapsed = (System.nanoTime() - startTime) / 1e9;
                System.out.println("\nTraining time (sec)\t" + elapsed);
            }
            else if (option.inf){
                Long startTime = System.nanoTime();

                Inferencer inferencer = new Inferencer(option);
                Model newModel = inferencer.inference();

                Double elapsed = (System.nanoTime() - startTime) / 1e9;
                System.out.println("Inferring time (sec)\t" + elapsed);
            }
        } catch (CmdLineException cle){
            System.out.println("Command line error: " + cle.getMessage());
            showHelp(parser);
            return;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (Exception e){
            System.out.println("Error in main: " + e.getMessage());
            e.printStackTrace();
            return;
        }
    }

    public static void showHelp(CmdLineParser parser){
        System.out.println("LDA [options ...] [arguments...]");
        parser.printUsage(System.out);
    }

}
