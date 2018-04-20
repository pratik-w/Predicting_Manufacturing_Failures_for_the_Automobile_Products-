package storm.ui;
import java.awt.BorderLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.Timer;

public class stormui{

	File file = null;
	BufferedReader reader = null;

	private Timer timer = null;
	private JTextArea textArea;
	private JTextField jtfFile;
	private String fileName;
	private JButton browse;
	private JFrame frame;

	public stormui() {
		textArea = new JTextArea(35, 70);
		frame = new JFrame("BOSCH");

		browse = new JButton("Select File and Submit");
		browse.addActionListener(new ShowOutputListener());

		jtfFile = new JTextField(25);
		jtfFile.addActionListener(new ShowOutputListener());

		timer = new Timer(800, new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				String line;

				try {
					if ((line = reader.readLine()) != null) {
						textArea.append(line + "\n");
					} else {
						// ((Timer) e.getSource()).stop();
					}
				} catch (IOException ex) {
					Logger.getLogger(stormui.class.getName()).log(Level.SEVERE, null, ex);
				}

			}
		});

		JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());

		panel.add(new JLabel("Select Input file: "), BorderLayout.LINE_START);
		panel.add(jtfFile, BorderLayout.CENTER);
		panel.add(browse, BorderLayout.LINE_END);
		panel.add(textArea, BorderLayout.SOUTH);

		JPanel panel2 = new JPanel();

		ImageIcon loading = new ImageIcon("ajax-loader.gif");

		JLabel jl = new JLabel("loading... ", loading, JLabel.CENTER);
		// jl.setBounds(10, 80, 180, 25);

		panel2.add(jl);
		panel2.setVisible(false);

		JPanel panel3 = new JPanel();

		ImageIcon anim = new ImageIcon("/s/chopin/a/grad/akmittal/Downloads/anim.gif");
		JLabel jl3 = new JLabel(anim, JLabel.CENTER);

		panel3.add(jl3);

		JPanel panel4 = new JPanel();

		ImageIcon boschlogo = new ImageIcon("/s/chopin/a/grad/akmittal/Downloads/bosch.jpg");
		Image image = boschlogo.getImage(); // transform it
		Image newimg = image.getScaledInstance(50, 50, java.awt.Image.SCALE_SMOOTH); // scale
																						// it
																						// the
																						// smooth
																						// way
		boschlogo = new ImageIcon(newimg); // transform it back
		JLabel jl4 = new JLabel(boschlogo, JLabel.CENTER);

		panel4.add(jl4);

		frame.add(panel4, BorderLayout.PAGE_START);

		frame.add(panel, BorderLayout.LINE_START);
		frame.add(panel2, BorderLayout.CENTER, 1);

		// frame.add(new JScrollPane(textArea), BorderLayout.CENTER);
		frame.add(panel3, BorderLayout.PAGE_END);

		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.pack();
		frame.setLocationByPlatform(true);
		frame.setVisible(true);
	}

	// private class ShowLogListener implements ActionListener {
	//
	// @Override
	// public void actionPerformed(ActionEvent e) {
	// JFileChooser chooser = new JFileChooser();
	// int result = chooser.showOpenDialog(frame);
	// if (result == JFileChooser.APPROVE_OPTION) {
	// file = chooser.getSelectedFile();
	// fileName = file.getName();
	// jtfFile.setText(fileName);
	// try {
	// reader = new BufferedReader(new FileReader(file));
	// } catch (FileNotFoundException ex) {
	// Logger.getLogger(ReadFile.class.getName()).log(Level.SEVERE, null, ex);
	// }
	// timer.start();
	// }
	// }
	// }

	private class ShowOutputListener implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent e) {
			JFileChooser chooser = new JFileChooser();
			int result = chooser.showOpenDialog(frame);
			if (result == JFileChooser.APPROVE_OPTION) {
				// file = chooser.getSelectedFile();
				// fileName = file.getName();
				// jtfFile.setText(fileName);
				// try {
				// reader = new BufferedReader(new FileReader(file));
				// } catch (FileNotFoundException ex) {
				// Logger.getLogger(ReadFile.class.getName()).log(Level.SEVERE,
				// null, ex);
				// }

				// Submit the script to run Storm
				System.out.println("Submit the script to run Storm");
				// try {
				// Process p = Runtime.getRuntime().exec(new
				// String[]{"bash","-c","ls ~/"});
				// } catch (IOException e1) {
				// // TODO Auto-generated catch block
				// e1.printStackTrace();
				// }

				List<String> commands = new ArrayList<String>();
				commands.add("refactor1.sh");
				// Add arguments
				System.out.println(commands);

				// Run macro on target
				ProcessBuilder pb = new ProcessBuilder(commands);
				pb.directory(new File("/s/chopin/a/grad/akmittal/Downloads"));
				pb.redirectErrorStream(true);
				Process process = null;

				try {
					process = pb.start();
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}

				// Read output
				StringBuilder out = new StringBuilder();
				BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line = null, previous = null;
				try {
					while ((line = br.readLine()) != null)
						if (!line.equals(previous)) {
							previous = line;
							out.append(line).append('\n');
							System.out.println(line);
						}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				//

				file = new File("/s/chopin/a/grad/akmittal/stormlogs/boschop.txt");


				while (!file.exists()) {
					System.out.println("No file Yet");
				}

				try {
					reader = new BufferedReader(new FileReader(file));
					frame.getComponent(0).setVisible(true);
					System.out.println(frame.getComponent(0).toString());
					timer.start();

				} catch (FileNotFoundException ex) {
					Logger.getLogger(stormui.class.getName()).log(Level.SEVERE, null, ex);
				}

			}
		}
	}

	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				new stormui();
			}
		});
	}
}
