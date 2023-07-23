// app.js

const express = require('express');
const bodyParser = require('body-parser');
const multer = require('multer');
const mongoose = require('mongoose');
const { Worker, isMainThread } = require('worker_threads');
const os = require('os');
const { spawn } = require('child_process');

const app = express();
const port = 3000;

// Database connection.....
const mongoURI = 'mongodb://localhost:27017/policy';
mongoose.connect(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Database Connected Successfully'))
  .catch(err => console.error('Error connecting to DB:', err));

//Schema Define here......
const agentSchema = new mongoose.Schema({
  agentName: String,
});

const userSchema = new mongoose.Schema({
  firstName: String,
  DOB: Date,
  address: String,
  phoneNumber: String,
  state: String,
  zipCode: String,
  email: String,
  gender: String,
  userType: String,
});

const accountSchema = new mongoose.Schema({
  accountName: String,
});

const policyCategorySchema = new mongoose.Schema({
  category_name: String,
});

const policyCarrierSchema = new mongoose.Schema({
  company_name: String,
});

const policyInfoSchema = new mongoose.Schema({
  policyNumber: String,
  policyStartDate: Date,
  policyEndDate: Date,
  policyCategory: { type: mongoose.Schema.Types.ObjectId, ref: 'PolicyCategory' },
  collectionId: { type: mongoose.Schema.Types.ObjectId, ref: 'UserAccount' },
  companyCollectionId: { type: mongoose.Schema.Types.ObjectId, ref: 'PolicyCarrier' },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
});


const Agent = mongoose.model('Agent', agentSchema);
const User = mongoose.model('User', userSchema);
const UserAccount = mongoose.model('UserAccount', accountSchema);
const PolicyCategory = mongoose.model('PolicyCategory', policyCategorySchema);
const PolicyCarrier = mongoose.model('PolicyCarrier', policyCarrierSchema);
const PolicyInfo = mongoose.model('PolicyInfo', policyInfoSchema);


const storage = multer.memoryStorage();
const upload = multer({ storage });


function processDataWithWorker(data) {

  return new Promise((resolve, reject) => {
    const worker = new Worker('./worker.js', { workerData: data });

    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}

//1.Upload XLSX/CSV
app.post('/uploadfile', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const fileBuffer = req.file.buffer;
    const originalFilename = req.file.originalname;

    const result = await processDataWithWorker({ fileBuffer, originalFilename });

    const { agents, users, accounts, policyCategories, policyCarriers, policyInfos } = result;

    await Agent.insertMany(agents);
    await User.insertMany(users);
    await UserAccount.insertMany(accounts);
    await PolicyCategory.insertMany(policyCategories);
    await PolicyCarrier.insertMany(policyCarriers);
    await PolicyInfo.insertMany(policyInfos);

    res.status(200).json({ message: 'File uploaded and data saved successfully' });
  } catch (err) {
    console.error('Error uploading file:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

//2.find policy info with the help of the username
app.get('/search/:username', async (req, res) => {
  try {
    const { username } = req.params;

    const user = await User.findOne({ firstName: username });

    if (!user) {
      return res.status(404).json({ statusCode: 404, error: 'User not found' });
    }

    const policyInfo = await PolicyInfo.find({ userId: user._id })
      .populate('policyCategory')
      .populate('collectionId')
      .populate('companyCollectionId');

    res.status(200).json({ statusCode: 200, message: "Fetch Successfully", user, policyInfo });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// 3: API for provide aggregated policy by each user
app.get('/aggregated-policy', async (req, res) => {
  try {

    const aggregatedData = await PolicyInfo.aggregate([
      {
        $group: {
          _id: '$userId',
          policyCount: { $sum: 1 },
          totalPolicyAmount: { $sum: '$policyAmount' },
        },
      },
      {
        $lookup: {
          from: 'users',
          localField: '_id',
          foreignField: '_id',
          as: 'user',
        },
      },
      {
        $unwind: '$user',
      },
      {
        $project: {
          _id: 0,
          userId: '$_id',
          userName: '$user.firstName',
          policyCount: 1,
          totalPolicyAmount: 1,
        },
      },
    ]);

    res.status(200).json({ statusCode: 200, message: "Fetch Successfully", result: aggregatedData });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

//Task 2..............
//1st Track real-time CPU utilization of the node server and on 70% usage restart the server.
const SERVER_RESTART_DELAY_MS = 5000;
const MAX_CPU_USAGE_PERCENTAGE = 70;

function checkCpuUsage() {
  const cpuUsage = os.loadavg()[0];
  console.log(`Current CPU Usage: ${cpuUsage.toFixed(2)}%`);

  if (cpuUsage > MAX_CPU_USAGE_PERCENTAGE) {
    console.log('CPU usage exceeds 70%. Restarting the server...');
    restartServer();
  } else {
    setTimeout(checkCpuUsage, 1000);
  }
}

function restartServer() {

  const nodeProcess = process.argv[0];
  const scriptPath = process.argv[1];
  const args = process.argv.slice(2);

  setTimeout(() => {
    const newServer = spawn(nodeProcess, [scriptPath, ...args], {
      detached: true,
      stdio: 'inherit',
    });

    newServer.unref();
    process.exit();
  }, SERVER_RESTART_DELAY_MS);
}

checkCpuUsage();


//2nd.. Create a post-service that takes the message, day, and time in body parameters and it inserts that message into DB at that particular day and time.

const messageSchema = new mongoose.Schema({
  message: String,
  scheduledTime: Date,
});

const Message = mongoose.model('Message', messageSchema);

app.use(bodyParser.json());

// Step 2: Create the post-service to schedule and insert messages into the database
app.post('/schedule-message', async (req, res) => {
  try {
    const { message, day, time } = req.body;

    const scheduledDateTime = new Date(`${day} ${time}`);

    const newMessage = new Message({
      message,
      scheduledTime: scheduledDateTime,
    });


    await newMessage.save();

    res.status(200).json({ statusCode: 200, message: 'Message scheduled and inserted' });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
